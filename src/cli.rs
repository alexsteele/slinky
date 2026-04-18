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
    Relay {
        #[command(subcommand)]
        command: RelayCommand,
    },
}

#[derive(Debug, Args)]
pub struct SetupArgs {
    #[arg(long)]
    pub sync_root: Option<PathBuf>,

    #[arg(long)]
    pub relay: Option<String>,
}

#[derive(Debug, Args)]
pub struct AddDeviceArgs {
    #[arg(long)]
    pub relay: Option<String>,
}

#[derive(Debug, Args)]
pub struct StartArgs {
    #[arg(long)]
    pub foreground: bool,
}

#[derive(Debug, Subcommand)]
pub enum RelayCommand {
    Serve(RelayServeArgs),
}

#[derive(Debug, Args)]
pub struct RelayServeArgs {
    #[arg(long)]
    pub bind: Option<String>,
}
