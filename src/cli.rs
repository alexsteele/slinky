use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "slinky")]
#[command(about = "Continuous folder sync engine")]
pub struct Cli {
    #[arg(long, global = true)]
    pub config: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Setup(SetupArgs),
    AddDevice(AddDeviceArgs),
    Start(StartArgs),
    Stop,
    Status,
    Coordinator {
        #[command(subcommand)]
        command: CoordinatorCommand,
    },
}

#[derive(Debug, Args)]
pub struct SetupArgs {
    #[arg(long)]
    pub sync_root: Option<PathBuf>,

    #[arg(long)]
    pub coordinator: Option<String>,
}

#[derive(Debug, Args)]
pub struct AddDeviceArgs {
    #[arg(long)]
    pub coordinator: Option<String>,
}

#[derive(Debug, Args)]
pub struct StartArgs {
    #[arg(long)]
    pub foreground: bool,
}

#[derive(Debug, Subcommand)]
pub enum CoordinatorCommand {
    Serve(CoordinatorServeArgs),
}

#[derive(Debug, Args)]
pub struct CoordinatorServeArgs {
    #[arg(long)]
    pub bind: Option<String>,
}
