mod common;
mod stat;
mod client;
mod server;
mod data;

use chrono::{Utc, DateTime};
use redis::AsyncCommands;
use tokio::runtime::Builder;
use common::{Settings, create_settings, code_name, fast_name};
use data::{DataHandlerSettings, DataHandler};
use client::client_processing;
use server::server_processing;
use log::{info, warn, error, debug};


async fn check_connection(settings: Settings, redis_c: redis::Client) {
    let key: String = format!("d-ch-{}-{}", code_name(&settings.client_name), fast_name());
    let interval = 60;
    let half = interval / 2;
    let mut r_con = redis_c.get_multiplexed_async_connection().await.unwrap();
    let mut index = 0 as u64;
    let exit_code = 1;

    loop {
        let in_dt: String = r_con.get(&key).await.unwrap_or("".to_string());
        tokio::time::sleep(tokio::time::Duration::from_secs(half)).await;
        let now = Utc::now().to_rfc3339();
        match r_con.set_ex::<String, String, ()>(key.clone(), now.clone(), interval).await {
            Ok(_) => {},
            Err(err) => {
                error!("Watcher found error: {}", err);
                std::process::exit(exit_code);
            }
        }
        if !in_dt.is_empty() {
            let dt =  match DateTime::parse_from_rfc3339(&in_dt) {
                Ok(dt) => {dt},
                Err(_) => {
                    warn!("Unexpected value in key {}: {}", key, in_dt);
                    index += 1;
                    continue;
                }
            };
            let duration = DateTime::parse_from_rfc3339(&now).unwrap() - dt;
            if duration.num_seconds() > interval as i64 {
                error!("Watcher got to many second without connections {}", duration.num_seconds());
                std::process::exit(exit_code);
            } else {
                debug!("Watcher flag created at {}", in_dt);
            }
        } else if index > 0 {
            error!("Watcher deserted a lag");
            std::process::exit(exit_code);
        }
        index += 1;
    }
}

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
    let main_client = r_client.clone();
    let mut set;
    if is_s {
        set = server_processing(settings.clone(), data_handler, r_client).await;
    } else {
        set = client_processing(settings.clone(), data_handler, r_client).await;
    };
    let settings = settings.clone();
    set.spawn(async move {
        check_connection(settings, main_client).await;
    });

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
