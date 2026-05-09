use anyhow::Result;
use clap::{Parser, Subcommand};
use platform_a2a::{PLATFORM_TOKEN_HEADER, require_platform_api_token, validate_endpoint};
use platform_domain::WorkflowRequest;

#[derive(Debug, Parser)]
#[command(name = "orchestrator-cli")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    ListRuntimes {
        #[arg(long, default_value = "http://127.0.0.1:9000")]
        control_plane: String,
    },
    SubmitWorkflow {
        #[arg(long, default_value = "http://127.0.0.1:9000")]
        control_plane: String,
        #[arg(long)]
        objective: String,
        #[arg(long, value_delimiter = ',')]
        constraints: Vec<String>,
    },
    WorkflowStatus {
        #[arg(long, default_value = "http://127.0.0.1:9000")]
        control_plane: String,
        #[arg(long)]
        workflow_id: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    require_platform_api_token()?;
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .connect_timeout(std::time::Duration::from_secs(10))
        .build()?;

    match cli.command {
        Command::ListRuntimes { control_plane } => {
            validate_endpoint(&control_plane)?;
            let body = http
                .get(format!("{control_plane}/runtimes"))
                .header(PLATFORM_TOKEN_HEADER, require_platform_api_token()?)
                .send()
                .await?
                .error_for_status()?
                .text()
                .await?;
            println!("{body}");
        }
        Command::SubmitWorkflow {
            control_plane,
            objective,
            constraints,
        } => {
            validate_endpoint(&control_plane)?;
            let request = WorkflowRequest {
                objective,
                constraints,
                context_id: None,
            };
            let body = http
                .post(format!("{control_plane}/workflows/submit"))
                .header(PLATFORM_TOKEN_HEADER, require_platform_api_token()?)
                .json(&request)
                .send()
                .await?
                .error_for_status()?
                .text()
                .await?;
            println!("{body}");
        }
        Command::WorkflowStatus {
            control_plane,
            workflow_id,
        } => {
            validate_endpoint(&control_plane)?;
            let body = http
                .get(format!("{control_plane}/workflows/{workflow_id}"))
                .header(PLATFORM_TOKEN_HEADER, require_platform_api_token()?)
                .send()
                .await?
                .error_for_status()?
                .text()
                .await?;
            println!("{body}");
        }
    }

    Ok(())
}
