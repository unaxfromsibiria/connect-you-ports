mod client;
mod data;
mod route;
mod server;
mod stat;
mod transport;
mod utils;
mod settings;

use log::{info};
use tokio::runtime::Builder;

async fn run(settings: &settings::Settings) -> Result<(), Box<dyn std::error::Error>> {
    if settings.is_server {
        server::run(settings.clone()).await;
    } else {
        client::run(settings.clone()).await;
    }
    Ok(())
}

fn install_shutdown_handler(is_server: bool) {
    let _ = ctrlc::set_handler(move || {
        info!("Received termination signal, shutting down...");
        if is_server {
            server::stop();
        } else {
            client::stop();
        }
    });
}

fn main() {
    env_logger::init();
    let bin_name = std::env::args().next().map(
        |a| std::path::Path::new(&a).file_name().map(
            |f| f.to_string_lossy().into_owned()
        ).unwrap_or_else(|| a.clone())
    ).unwrap_or_else(|| "someqtt".to_string());

    let args: Vec<String> = std::env::args().skip(1).collect();

    let overrides = match settings::parse_cli_args(&args) {
        Ok(settings::CliParseResult::Help) => {
            utils::print_usage(&bin_name);
            return;
        }
        Ok(settings::CliParseResult::Args(o)) => o,
        Err(e) => {
            eprintln!("Error: {}", e);
            println!();
            utils::print_usage(&bin_name);
            std::process::exit(1);
        }
    };

    if let Some(key_size) = overrides.genkey {
        println!("{}", settings::generate_cipher_key(key_size));
        return;
    }

    if overrides.stat {
        match utils::show_stat_table(&settings::default_stat_filepath()) {
            Ok(()) => (),
            Err(e) => {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
        return;
    }
    let settings = settings::create_settings(&overrides);
    utils::main_info(&settings);
    // Workers optional: 0/unset means tokio auto-detects CPU cores, otherwise the explicit count applies.
    let mut builder = Builder::new_multi_thread();
    builder.enable_all();
    if settings.workers > 0 {
        builder.worker_threads(settings.workers);
        info!("Tokio worker threads: {}", settings.workers);
    } else {
        info!("Tokio worker threads: default (auto-detected CPU cores)");
    }
    let rt = builder.build().expect("Failed to build Tokio runtime");
    install_shutdown_handler(settings.is_server);
    let _ = rt.block_on(run(&settings));
}
