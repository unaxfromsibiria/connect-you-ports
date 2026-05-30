mod common;
mod client;
mod data;
mod stat;
mod server;

use tokio::time::{Instant, sleep};
use tokio::{runtime::Builder, task::JoinSet};
use common::{Settings, create_settings};
use client::run as run_client;
use stat::{show_stats, update_metric};
use server::run_server;
use log::{info, error};

/// Starts the server task within the provided JoinSet.
async fn server(settings: Settings, tasks: &mut JoinSet<()>) {
    tasks.spawn(async move {
        run_server(settings).await;
    });
}

/// Runs the main application logic, handling server or client mode based on settings.
async fn run(settings: &Settings) -> Result<(), Box<dyn std::error::Error>> {
    let is_s = settings.is_server;
    let stat_show_interval = settings.stat_delay;
    let target = format!("{}:{}", settings.server_host, settings.server_port);
    let mode_str = if is_s { "server" } else { "client" };
    info!(
        "<<<<Starting>>>> Mode: {} | Loading Level: {} | Buffer Size: {}",
        mode_str, settings.loading_level, settings.buffer_size
    );
    info!("Client: {} | Target: {}", settings.client_name, target);
    let mut set = JoinSet::new();
    let settings = settings.clone();
    if is_s {
        server(settings, &mut set).await;
    } else {
        run_client(settings, &mut set).await;
    }

    set.spawn(async move {
        let start_time = Instant::now();
        loop {
            sleep(stat_show_interval).await;
            let elapsed = start_time.elapsed();
            let uptime_min = elapsed.as_secs() / 60;
            update_metric("uptime", uptime_min as usize).await;
            show_stats().await;
        }
    });

    while let Some(res) = set.join_next().await {
        match res {
            Ok(val) => {
                info!("Task completed with result: {:?}", val);
            }
            Err(err) => {
                error!("Task failed: {}", err);
            }
        }
    }
    Ok(())
}

/// Entry point for the application.
fn main() {
    env_logger::init();
    let settings = create_settings();
    let rt = Builder::new_multi_thread().worker_threads(
        settings.workers
    ).enable_all().build().unwrap();    
    info!("Tokio thread count: {}", settings.workers);
    let _ = rt.block_on(run(&settings));
}
