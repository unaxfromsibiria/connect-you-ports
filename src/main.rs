mod common;
mod stat;
mod client;
mod server;
mod data;

use tokio::runtime::Builder;
use common::{Settings, create_settings};
use data::{DataHandlerSettings, DataHandler};
use client::client_processing;
use server::server_processing;
use log::{info, warn, error};

async fn run(settings: &Settings) -> Result<(), Box<dyn std::error::Error>> {
    let uri = settings.redis_conn.clone();
    let r_client = match redis::Client::open(uri) {
        Ok(client) => client,
        Err(err) => {
            error!("Failed to connect to Redis: {}", err);
            return Err(err.into());
        }
    };
    let is_s = settings.is_server;
    info!("Mode: {}", if is_s {"server"} else {"client"});
    if !is_s {
        info!("Client: {}", settings.client_name);
    }
    let mut data_handler = DataHandlerSettings::new();
    if !data_handler.setup(&settings) {
        error!("Wrong settings for cipher");
        return Ok(());
    }
    let mut set;
    if is_s {
        set = server_processing(settings.clone(), data_handler, r_client).await;
    } else {
        set = client_processing(settings.clone(), data_handler, r_client).await;
    };
    while let Some(res) = set.join_next().await {
        match res {
            Ok(val) => {
                info!("Task done with result: {:?}", val);
            }
            Err(err) => {
                warn!("A task panicked or was cancelled: {}", err);
            }
        }
    }
    Ok(())
}


fn main() {
    env_logger::init();
    let settings = create_settings();
    let rt = Builder::new_multi_thread().worker_threads(
        settings.workers
    ).enable_all().build().unwrap();
    info!("Tokio thread count: {}", settings.workers);
    let _ = rt.block_on(run(&settings));
}
