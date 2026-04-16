use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use clap::Parser;
use slinky::cli::{Cli, Command, CoordinatorCommand};
use slinky::core::{Config, DeviceCredentials};
use slinky::device::Device;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let cli = Cli::parse();

    if let Err(error) = run(cli).await {
        eprintln!("error: {error:?}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<(), slinky::services::SyncError> {
    match cli.command {
        Command::Setup(args) => {
            let home_dir = default_home_dir()?;
            let config_path = cli.config.unwrap_or(home_dir.join("config.toml"));
            let sync_root = match args.sync_root {
                Some(path) => path,
                None => home_dir.join("sync-root"),
            };
            let config = write_setup_config(&config_path, &sync_root)?;

            println!("wrote config to {}", config_path.display());
            println!("sync root: {}", config.sync_root.display());
            println!("repo id: {}", config.repo_id);
            println!("device id: {}", config.device_id);
            if let Some(coordinator) = args.coordinator {
                println!("coordinator placeholder noted but not wired yet: {coordinator}");
            }
        }
        Command::AddDevice(args) => {
            println!("add-device scaffold: {:?}", args);
        }
        Command::Start(args) => {
            let config_path = cli.config.unwrap_or(default_config_path()?);
            let mut device = Device::open_path(&config_path).await?;

            if !args.foreground {
                return Err(slinky::services::SyncError::InvalidState(
                    "background start is not wired yet; use --foreground".into(),
                ));
            }

            device.start().await?;
            println!("slinky started in foreground mode; press Ctrl-C to stop");
            tokio::signal::ctrl_c()
                .await
                .map_err(|err| slinky::services::SyncError::InvalidState(err.to_string()))?;
            device.stop().await?;
            device.join().await?;
        }
        Command::Stop => {
            println!("stop scaffold");
        }
        Command::Status => {
            println!("status scaffold");
        }
        Command::Coordinator {
            command: CoordinatorCommand::Serve(args),
        } => {
            println!("coordinator serve scaffold: {:?}", args);
        }
    }
    Ok(())
}

fn default_config_path() -> Result<PathBuf, slinky::services::SyncError> {
    Ok(default_home_dir()?.join("config.toml"))
}

fn default_home_dir() -> Result<PathBuf, slinky::services::SyncError> {
    let home = std::env::var("HOME")
        .map(PathBuf::from)
        .map_err(|_| slinky::services::SyncError::InvalidState("HOME is not set".into()))?;
    Ok(home.join(".slinky"))
}

fn write_setup_config(
    config_path: &Path,
    sync_root: &Path,
) -> Result<Config, slinky::services::SyncError> {
    if config_path.exists() {
        return Err(slinky::services::SyncError::InvalidState(format!(
            "config already exists at {}",
            config_path.display()
        )));
    }

    let config_dir = config_path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(config_dir)?;
    std::fs::create_dir_all(sync_root)?;
    let keys_dir = config_dir.join("keys");
    std::fs::create_dir_all(&keys_dir)?;

    let repo_id = format!("repo-{}", unique_suffix()?);
    let device_id = format!("device-{}", unique_suffix()?);
    let private_key_path = keys_dir.join(format!("{device_id}.key"));

    std::fs::write(
        &private_key_path,
        format!("placeholder-private-key-for-{device_id}\n"),
    )?;

    let config = Config {
        sync_root: sync_root.to_path_buf(),
        repo_id,
        device_id: device_id.clone(),
        credentials: DeviceCredentials {
            public_key: format!("placeholder-public-key-for-{device_id}"),
            private_key_path,
        },
    };

    let contents = toml::to_string_pretty(&config)
        .map_err(|err| slinky::services::SyncError::InvalidState(err.to_string()))?;
    std::fs::write(config_path, contents)?;

    Ok(config)
}

fn unique_suffix() -> Result<String, slinky::services::SyncError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| slinky::services::SyncError::InvalidState(err.to_string()))?;
    Ok(format!("{:x}-{:x}", now.as_nanos(), std::process::id()))
}
