[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet("start", "stop")]
    [string]$Action = "start"
)

$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$RunRoot = Join-Path $Root "target\platform-runtime"
$LogRoot = Join-Path $RunRoot "logs"
$PidFile = Join-Path $RunRoot "processes.json"
$ControlPlaneBaseUrl = "http://127.0.0.1:9000"

function Get-TrackedProcesses {
    if (-not (Test-Path -LiteralPath $PidFile)) {
        return @()
    }

    $raw = Get-Content -LiteralPath $PidFile -Raw
    if ([string]::IsNullOrWhiteSpace($raw)) {
        return @()
    }

    $data = $raw | ConvertFrom-Json
    if ($data -is [System.Array]) {
        return $data
    }

    return @($data)
}

function Remove-TrackingFile {
    if (Test-Path -LiteralPath $PidFile) {
        Remove-Item -LiteralPath $PidFile -Force
    }
}

function Stop-TrackedProcesses {
    param(
        [switch]$Quiet
    )

    $tracked = Get-TrackedProcesses
    if ($tracked.Count -eq 0) {
        if (-not $Quiet) {
            Write-Host "No tracked platform processes are running."
        }
        Remove-TrackingFile
        return
    }

    $stopped = 0
    foreach ($entry in $tracked) {
        try {
            $process = Get-Process -Id $entry.Id -ErrorAction Stop
        } catch {
            continue
        }

        $startedAt = $null
        try {
            $startedAt = $process.StartTime.ToUniversalTime().ToString("o")
        } catch {
            continue
        }

        if ($startedAt -ne $entry.StartedAtUtc) {
            continue
        }

        Stop-Process -Id $process.Id -ErrorAction Stop
        $stopped += 1
        if (-not $Quiet) {
            Write-Host ("Stopped {0} (PID {1})." -f $entry.Name, $process.Id)
        }
    }

    Remove-TrackingFile
    if (-not $Quiet) {
        if ($stopped -eq 0) {
            Write-Host "No tracked platform processes were still running."
        } else {
            Write-Host ("Stopped {0} platform process(es)." -f $stopped)
        }
    }
}

function Wait-HttpReady {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Url,
        [int]$TimeoutSeconds = 20
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            $response = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 3
            if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 500) {
                return
            }
        } catch {
        }

        Start-Sleep -Milliseconds 500
    }

    throw "Timed out waiting for $Url"
}

function Start-ManagedProcess {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name,
        [Parameter(Mandatory = $true)]
        [string]$FilePath,
        [Parameter(Mandatory = $true)]
        [string[]]$ArgumentList,
        [Parameter(Mandatory = $true)]
        [string]$HealthUrl
    )

    $stdout = Join-Path $LogRoot "$Name.stdout.log"
    $stderr = Join-Path $LogRoot "$Name.stderr.log"

    $process = Start-Process `
        -FilePath $FilePath `
        -ArgumentList $ArgumentList `
        -WorkingDirectory $Root `
        -RedirectStandardOutput $stdout `
        -RedirectStandardError $stderr `
        -WindowStyle Hidden `
        -PassThru

    Start-Sleep -Milliseconds 700
    if ($process.HasExited) {
        $errorText = ""
        if (Test-Path -LiteralPath $stderr) {
            $errorText = (Get-Content -LiteralPath $stderr -Raw).Trim()
        }
        if (-not $errorText -and (Test-Path -LiteralPath $stdout)) {
            $errorText = (Get-Content -LiteralPath $stdout -Raw).Trim()
        }
        if (-not $errorText) {
            $errorText = "Process exited before becoming healthy."
        }
        throw "$Name failed to start. $errorText"
    }

    Wait-HttpReady -Url $HealthUrl

    return [pscustomobject]@{
        Name         = $Name
        Id           = $process.Id
        StartedAtUtc = $process.StartTime.ToUniversalTime().ToString("o")
        HealthUrl    = $HealthUrl
        StdOutLog    = $stdout
        StdErrLog    = $stderr
    }
}

function Start-Platform {
    $null = Get-Command cargo -ErrorAction Stop

    if (-not $env:PLATFORM_API_TOKEN) {
        $env:PLATFORM_API_TOKEN = "local-review-token"
    }
    if (-not $env:PLATFORM_ALLOWED_HOSTS) {
        $env:PLATFORM_ALLOWED_HOSTS = "127.0.0.1,localhost"
    }
    if (-not $env:RUST_LOG) {
        $env:RUST_LOG = "info"
    }

    New-Item -ItemType Directory -Path $RunRoot -Force | Out-Null
    New-Item -ItemType Directory -Path $LogRoot -Force | Out-Null

    Stop-TrackedProcesses -Quiet
    Get-ChildItem -LiteralPath $LogRoot -File -ErrorAction SilentlyContinue | Remove-Item -Force

    Write-Host "Building workspace..."
    & cargo build --workspace
    if ($LASTEXITCODE -ne 0) {
        throw "cargo build failed."
    }

    $binRoot = Join-Path $Root "target\debug"
    $services = @(
        @{
            Name = "control-plane"
            FilePath = (Join-Path $binRoot "control-plane.exe")
            Args = @("--bind", "127.0.0.1:9000")
            HealthUrl = "$ControlPlaneBaseUrl/health"
        },
        @{
            Name = "runtime-planner"
            FilePath = (Join-Path $binRoot "runtime-node.exe")
            Args = @("--bind", "127.0.0.1:9101", "--public-endpoint", "http://127.0.0.1:9101/a2a", "--control-plane", $ControlPlaneBaseUrl, "--runtime-id", "planner-1", "--agent-id", "planner-1", "--profile", "planner", "--auto-register")
            HealthUrl = "http://127.0.0.1:9101/health"
        },
        @{
            Name = "runtime-builder"
            FilePath = (Join-Path $binRoot "runtime-node.exe")
            Args = @("--bind", "127.0.0.1:9102", "--public-endpoint", "http://127.0.0.1:9102/a2a", "--control-plane", $ControlPlaneBaseUrl, "--runtime-id", "builder-1", "--agent-id", "builder-1", "--profile", "builder", "--auto-register")
            HealthUrl = "http://127.0.0.1:9102/health"
        },
        @{
            Name = "runtime-reviewer"
            FilePath = (Join-Path $binRoot "runtime-node.exe")
            Args = @("--bind", "127.0.0.1:9103", "--public-endpoint", "http://127.0.0.1:9103/a2a", "--control-plane", $ControlPlaneBaseUrl, "--runtime-id", "reviewer-1", "--agent-id", "reviewer-1", "--profile", "reviewer", "--auto-register")
            HealthUrl = "http://127.0.0.1:9103/health"
        },
        @{
            Name = "runtime-synthesizer"
            FilePath = (Join-Path $binRoot "runtime-node.exe")
            Args = @("--bind", "127.0.0.1:9104", "--public-endpoint", "http://127.0.0.1:9104/a2a", "--control-plane", $ControlPlaneBaseUrl, "--runtime-id", "synthesizer-1", "--agent-id", "synthesizer-1", "--profile", "synthesizer", "--auto-register")
            HealthUrl = "http://127.0.0.1:9104/health"
        }
    )

    foreach ($service in $services) {
        if (-not (Test-Path -LiteralPath $service.FilePath)) {
            throw "Missing binary: $($service.FilePath)"
        }
    }

    $started = New-Object System.Collections.Generic.List[object]
    try {
        foreach ($service in $services) {
            Write-Host ("Starting {0}..." -f $service.Name)
            $started.Add((Start-ManagedProcess -Name $service.Name -FilePath $service.FilePath -ArgumentList $service.Args -HealthUrl $service.HealthUrl))
        }

        $started | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $PidFile -Encoding UTF8

        Wait-HttpReady -Url "$ControlPlaneBaseUrl/"

        Write-Host ""
        Write-Host "Platform is running in background processes."
        Write-Host ("Web:            {0}/" -f $ControlPlaneBaseUrl)
        Write-Host ("Health:         {0}/health" -f $ControlPlaneBaseUrl)
        Write-Host ("Runtimes API:   {0}/runtimes  (requires x-platform-token)" -f $ControlPlaneBaseUrl)
        Write-Host ("Workflow API:   {0}/workflows (requires x-platform-token)" -f $ControlPlaneBaseUrl)
        Write-Host ("Logs:           {0}" -f $LogRoot)
        Write-Host "Stop command:   .\stop-platform.cmd"
        Write-Host ""
        Write-Host "CLI examples:"
        Write-Host ("  cargo run -p cli -- list-runtimes --control-plane {0}" -f $ControlPlaneBaseUrl)
        Write-Host ("  cargo run -p cli -- submit-workflow --control-plane {0} --objective ""Build a Rust platform for capability-aware agent cowork using A2A direct dialogue"" --constraints rust-only,a2a-direct-dialogue,capability-based-assignment,review-loop" -f $ControlPlaneBaseUrl)
    } catch {
        $started | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $PidFile -Encoding UTF8
        Stop-TrackedProcesses -Quiet
        throw
    }
}

try {
    switch ($Action) {
        "start" { Start-Platform }
        "stop" { Stop-TrackedProcesses }
    }
} catch {
    Write-Error $_
    exit 1
}
