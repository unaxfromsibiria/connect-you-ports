use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::{interval, sleep};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use uuid::Uuid;

use crate::data::{DataHandler, DataHandlerSettings};
use crate::route::{add_route, exists, remove_route, run_cleanup, send_data, set_channel_size};
use crate::settings::{code_name, fast_name, part_uuid, LoadingParams, Settings, TaskResultEnum};
use crate::stat::{add_connection, lost_connection, update_metric, update_traffic_stats};

async fn server_connection(
    settings: Arc<Settings>,
    service_name: String,
    connection_info: String,
    addr: String,
    data_handler: Arc<DataHandlerSettings>,
    mut to_server_channel: mpsc::Receiver<Bytes>,
) {
    let mut reconnect_attempts: usize = 0;
    let mut format_error_count: usize = 0;
    let mut route_notfound_count: usize = 0;
    let mut transfer_in: usize = 0;
    let mut transfer_out: usize = 0;
    let mut transfer_error: usize = 0;
    let update_stat_delay = settings.reconnect_delay(0) * 2;
    let stat_key_all = format!("out-c-{}", service_name);
    let metric_no_route_key = format!("route-err-{}", connection_info);
    let metric_format_err_key = format!("format-error-{}", connection_info);
    let conn_key = format!("to-serv-out-{}", connection_info);
    let mut done = false;
    add_connection(&conn_key).await;

    // Periodic stat flusher: interval ticks are scheduled on the tokio timer wheel, so they fire even when I/O stays hot.
    let mut stat_tick = interval(update_stat_delay);
    loop {
        match TcpStream::connect(&addr).await {
            Ok(stream) => {
                info!("Connection for {} established", connection_info);
                let mut framed = Framed::new(stream, LengthDelimitedCodec::new());
                loop {
                    tokio::select! {
                        frame_opt = framed.next() => match frame_opt {
                            Some(Ok(frame)) => {
                                let n = frame.as_ref().len();
                                reconnect_attempts = 0;
                                match data_handler.load_data_message(frame.as_ref()) {
                                    Ok((msg, transfer)) => {
                                        let with_client = send_data(&transfer, &msg.data).await;
                                        if msg.data.is_empty() || transfer.is_nil() {
                                            done = true;
                                            break;
                                        }
                                        if with_client {
                                            transfer_in += n;
                                        } else {
                                            route_notfound_count += 1;
                                        }
                                    },
                                    Err(err) => {
                                        error!("Wrong message format {} in connection: {}", err, connection_info);
                                        format_error_count += 1;
                                    },
                                }
                            },
                            Some(Err(err)) => {
                                error!("Read error {} in connection: {}", err, connection_info);
                                break;
                            },
                            None => {
                                info!("Connection closed for {} in {}", addr, connection_info);
                                done = true;
                                break;
                            },
                        },
                        Some(data) = to_server_channel.recv() => {
                            let iter_n = data.len();
                            if framed.send(data).await.is_err() {
                                error!("Send error in connection: {}", connection_info);
                                transfer_error += 1;
                                break;
                            } else {
                                transfer_out += iter_n;
                            }
                        },
                        // Periodic statistics update (the immediate first tick is a no-op while counters are zero).
                        _ = stat_tick.tick() => {
                            if transfer_in > 0 || transfer_out > 0 || transfer_error > 0 {
                                update_traffic_stats(&stat_key_all, transfer_in, transfer_out, transfer_error).await;
                                (transfer_in, transfer_out, transfer_error) = (0, 0, 0);
                            }
                            if route_notfound_count > 0 {
                                update_metric(&metric_no_route_key, route_notfound_count).await;
                                route_notfound_count = 0;
                            }
                            if format_error_count > 0 {
                                update_metric(&metric_format_err_key, format_error_count).await;
                                format_error_count = 0;
                            }
                        },
                    }
                }
            },
            Err(err) => {
                error!("Connection error {} in connection: {}", err, connection_info);
                transfer_error += 1;
            }
        }
        if transfer_in > 0 || transfer_out > 0 || transfer_error > 0 {
            update_traffic_stats(&stat_key_all, transfer_in, transfer_out, transfer_error).await;
        }
        if route_notfound_count > 0 {
            update_metric(&metric_no_route_key, route_notfound_count).await;
        }
        if format_error_count > 0 {
            update_metric(&metric_format_err_key, format_error_count).await;
        }
        if done {
            break;
        } else {
            let delay = settings.reconnect_delay(reconnect_attempts);
            warn!(
                "Reconnection attempt {} in {} (after: {} ms)",
                connection_info,
                reconnect_attempts + 1,
                delay.as_millis()
            );
            sleep(delay).await;
            reconnect_attempts += 1;
        }
    }
    lost_connection(&conn_key).await;
}

async fn tcp_client_processing(settings: &Settings, tasks: &mut JoinSet<TaskResultEnum>) {
    let tcp_keys: Vec<_> = settings.tcp_sockets.keys().cloned().collect();
    for serv_name in settings.udp_sockets.keys() {
        if tcp_keys.contains(serv_name) {
            error!("Configuration conflict: service '{}' is defined in both UDP and TCP sockets", serv_name);
            return;
        }
    }

    let data_handler = Arc::new(DataHandlerSettings::new(settings));
    for (service_name, ip_port_map) in settings.tcp_sockets.iter() {
        let Some((with_ip, with_port)) = ip_port_map.iter().next() else {
            error!("No IP-Port mapping found for server {}", service_name);
            continue;
        };
        let service_code = code_name(service_name);
        let ip = *with_ip;
        let port = *with_port;
        let service_name = service_name.clone();
        let settings_arc = Arc::new(settings.clone());
        let data_handler_local = data_handler.clone();

        tasks.spawn(async move {
            let idle_limit = settings_arc.idle_tcp_limit;
            let stat_save_iter = settings_arc.stat_save_iter;
            let buffer_size = settings_arc.buffer_size;
            let wait_before_close_time = settings_arc.collect_message_timeout(true);
            let (_, service_out_channel_size) = settings_arc.channel_size();
            let addr = format!("{}:{}", settings_arc.server_host, settings_arc.server_port);

            let listener = match TcpListener::bind((ip, port)).await {
                Ok(listener) => listener,
                Err(err) => {
                    error!("Failed to bind TCP listener to {}:{}: {}", ip, port, err);
                    update_metric("bind-listener-error", 1).await;
                    return TaskResultEnum::WorkerDone;
                }
            };
            info!(
                "Listening on tcp://{}:{} service {} ({})",
                ip,
                port,
                service_name,
                service_code
            );

            loop {
                let (stream, c_addr) = match listener.accept().await {
                    Ok(result) => result,
                    Err(err) => {
                        error!("Failed to accept TCP connection: {}", err);
                        break;
                    }
                };
                let transfer = fast_name();
                let conn_info = c_addr.ip().to_string();
                let s_code = service_code;
                let serv_name = service_name.clone();
                let stat_key = format!("in-{}-{}", service_name, ip);
                let service_name_display = format!("{} ({})", service_name, part_uuid(&s_code));
                let server_addr = addr.clone();
                let data_handler_conn = data_handler_local.clone();
                let data_handler_out = data_handler_local.clone();
                let settings_iter = settings_arc.clone();
                let (serv_tx, serv_rx) = mpsc::channel(service_out_channel_size);

                info!("New connection {} in {} from {}", transfer, service_name_display, c_addr);
                let from_client_channel = add_route(&transfer).await;

                tokio::spawn(async move {
                    server_connection(
                        settings_iter,
                        serv_name,
                        conn_info,
                        server_addr,
                        data_handler_out,
                        serv_rx,
                    ).await;
                });
                tokio::spawn(async move {
                    tcp_connection_processing(
                        c_addr,
                        transfer,
                        s_code,
                        service_name_display,
                        stat_key,
                        from_client_channel,
                        stream,
                        data_handler_conn,
                        serv_tx,
                        buffer_size,
                        idle_limit,
                        stat_save_iter,
                        wait_before_close_time,
                    ).await;
                });
            }
            TaskResultEnum::WorkerDone
        });
    }
}

async fn tcp_connection_processing(
    c_addr: SocketAddr,
    transfer: Uuid,
    service_code: Uuid,
    service_name: String,
    stat_key: String,
    mut from_client_channel: mpsc::Receiver<(Uuid, Bytes)>,
    stream: TcpStream,
    data_handler: Arc<DataHandlerSettings>,
    to_server_channel: mpsc::Sender<Bytes>,
    buffer_size: usize,
    idle_limit: std::time::Duration,
    stat_save_iter: std::time::Duration,
    wait_before_close_time: std::time::Duration,
) {
    add_connection(&stat_key).await;
    let t_inf = part_uuid(&transfer);
    let mut with_quit = false;
    let mut buf = vec![0u8; buffer_size];
    let (mut reader, mut writer) = tokio::io::split(stream);
    let (mut in_bytes, mut out_bytes, mut error_count) = (0usize, 0usize, 0usize);

    // Periodic stat flusher: interval ticks are scheduled on the tokio timer wheel, so they fire even when I/O stays hot.
    let mut stat_tick = interval(stat_save_iter);
    loop {
        tokio::select! {
            n = reader.read(&mut buf) => match n {
                Ok(n) if n >= 1 => {
                    out_bytes += n;
                    let data = data_handler.make_data_message(&buf[..n], &service_code, &transfer);
                    if to_server_channel.send(data).await.is_err() {
                        warn!("Failed to send data to server channel {} in {}", t_inf, service_name);
                        break;
                    } else {
                        with_quit = true;
                    }
                },
                Ok(_) => {
                    info!("Closing TCP connection for client {} in {} {}", c_addr, t_inf, service_name);
                    break;
                },
                Err(err) => {
                    error_count += 1;
                    error!("Failed to read from client {} in {}: {}", c_addr, service_name, err);
                    break;
                }
            },
            Some((_, data)) = from_client_channel.recv() => {
                if data.is_empty() {
                    info!("Closing connection {} client {} by request", c_addr, t_inf);
                    with_quit = false;
                    break;
                }
                if writer.write_all(&data).await.is_err() {
                    error_count += 1;
                    error!("Failed to write to client {} in {}", t_inf, service_name);
                    break;
                } else {
                    in_bytes += data.len();
                }
            },
            _ = sleep(idle_limit) => {
                warn!("Idle timeout for TCP connection {} client {}", t_inf, c_addr);
                break;
            },
            // Periodic traffic statistics update (the immediate first tick is a no-op while counters are zero).
            _ = stat_tick.tick() => {
                if in_bytes + out_bytes + error_count > 0 {
                    update_traffic_stats(&stat_key, in_bytes, out_bytes, error_count).await;
                    (in_bytes, out_bytes, error_count) = (0, 0, 0);
                }
            }
        }
    }

    if in_bytes + out_bytes + error_count > 0 {
        update_traffic_stats(&stat_key, in_bytes, out_bytes, error_count).await;
    }
    lost_connection(&stat_key).await;
    info!("Stopping connection handler for {} in {}", t_inf, service_name);

    loop {
        tokio::select! {
            _ = sleep(wait_before_close_time) => {
                if with_quit {
                    let data = data_handler.make_quit_message(&service_code, &transfer);
                    let _ = to_server_channel.send(data).await;
                }
                break;
            },
            Some((_, data)) = from_client_channel.recv() => {
                if data.is_empty() {
                    with_quit = false;
                    continue;
                }
                if writer.write_all(&data).await.is_err() {
                    error_count += 1;
                }
            },
        }
    }
    if in_bytes + out_bytes + error_count > 0 {
        update_traffic_stats(&stat_key, in_bytes, out_bytes, error_count).await;
    }
    remove_route(&transfer).await;
}

async fn udp_client_processing(settings: &Settings, tasks: &mut JoinSet<TaskResultEnum>) {
    let data_handler = Arc::new(DataHandlerSettings::new(settings));
    for (service_name, ip_port_map) in settings.udp_sockets.iter() {
        let Some((with_ip, with_port)) = ip_port_map.iter().next() else {
            error!("No IP-Port mapping found for UDP server '{}'", service_name);
            continue;
        };
        let service_code = code_name(service_name);
        let ip = *with_ip;
        let port = *with_port;
        let service_name = service_name.clone();
        let settings_arc = Arc::new(settings.clone());
        let data_handler_local = data_handler.clone();

        tasks.spawn(async move {
            let buffer_size = settings_arc.buffer_size;
            let min_delay = settings_arc.service_delay();
            let idle_limit = settings_arc.idle_udp_limit;
            let stat_save_iter = settings_arc.stat_save_iter;
            let client_name: String = settings_arc.client_name.clone();
            let (_, service_out_channel_size) = settings_arc.channel_size();

            let server_socket = match UdpSocket::bind((ip, port)).await {
                Ok(result) => result,
                Err(err) => {
                    error!("Failed to bind UDP socket for service '{}': {}", service_name, err);
                    update_metric("bind-listener-error", 1).await;
                    return TaskResultEnum::WorkerDone;
                }
            };
            info!(
                "Listening on udp://{}:{} service {} ({})",
                ip, port, service_name, service_code
            );

            let socket_arc = Arc::new(server_socket);
            let mut buf = vec![0u8; buffer_size];
            let server_channels: Arc<tokio::sync::RwLock<HashMap<Uuid, mpsc::Sender<Bytes>>>> =
                Arc::new(tokio::sync::RwLock::new(HashMap::new()));
            loop {
                let (n, peer) = match socket_arc.recv_from(&mut buf).await {
                    Ok(res) => res,
                    Err(err) => {
                        error!("UDP receive error for service {}: {}", service_name, err);
                        sleep(min_delay).await;
                        continue;
                    }
                };
                if n < 1 {
                    info!("Empty UDP packet from peer {}", peer);
                    continue;
                }
                let transfer = code_name(&format!("{}-{}-{}", service_code, client_name, peer));
                let data = data_handler_local.make_data_message(&buf[..n], &service_code, &transfer);
                if !exists(&transfer).await {
                    info!(
                        "New UDP peer {} in '{}'",
                        part_uuid(&transfer), service_name
                    );
                    let from_client_channel = add_route(&transfer).await;
                    let (serv_tx, serv_rx) = mpsc::channel(service_out_channel_size);
                    server_channels.write().await.insert(transfer, serv_tx);
                    let conn_info = peer.ip().to_string();
                    let settings_iter = settings_arc.clone();
                    let data_handler_iter = data_handler_local.clone();
                    let s_name = service_name.clone();
                    let peer_service_name = service_name.clone();
                    let server_addr_iter = format!("{}:{}", settings_iter.server_host, settings_iter.server_port);
                    tokio::spawn(async move {
                        server_connection(
                            settings_iter,
                            s_name,
                            conn_info,
                            server_addr_iter,
                            data_handler_iter,
                            serv_rx,
                        ).await;
                    });

                    let socket = Arc::clone(&socket_arc);
                    let channels_clone = Arc::clone(&server_channels);
                    tokio::spawn(async move {
                        udp_peer_processing(
                            peer,
                            transfer,
                            peer_service_name,
                            from_client_channel,
                            socket,
                            channels_clone,
                            min_delay,
                            idle_limit,
                            stat_save_iter,
                        ).await;
                    });
                }

                // Read lock + cloned sender: the map is read on every packet, and sending needs no mutation.
                let tx_opt = server_channels.read().await.get(&transfer).cloned();
                match tx_opt {
                    Some(tx) => {
                        if let Err(err) = tx.send(data).await {
                            warn!(
                                "Failed to send data to server channel {} in '{}': {}",
                                part_uuid(&transfer), service_name, err
                            );
                            let mut channels_guard = server_channels.write().await;
                            match channels_guard.remove(&transfer) {
                                Some(_) => info!(
                                    "Removed server channel for transfer {} in '{}'",
                                    part_uuid(&transfer), service_name
                                ),
                                None => warn!(
                                    "Server channel for transfer {} was already gone before remove in '{}'",
                                    part_uuid(&transfer), service_name
                                ),
                            }
                        }
                    },
                    None => error!(
                        "No route for transfer {} in '{}'",
                        part_uuid(&transfer), service_name
                    ),
                }
            }

        });
    }
}


async fn udp_peer_processing(
    peer: SocketAddr,
    transfer: Uuid,
    service_name: String,
    mut from_client_channel: mpsc::Receiver<(Uuid, Bytes)>,
    socket: Arc<UdpSocket>,
    server_channels: Arc<tokio::sync::RwLock<HashMap<Uuid, mpsc::Sender<Bytes>>>>,
    min_delay: Duration,
    idle_limit: Duration,
    stat_save_iter: Duration,
) {
    let t_inf = part_uuid(&transfer);
    let mut in_bytes = 0;

    // Periodic stat flusher: interval ticks are scheduled on the tokio timer wheel, so they fire even when I/O stays hot.
    let mut stat_tick = interval(stat_save_iter);
    loop {
        tokio::select! {
            // Send data from server connection to peer
            Some((_, data)) = from_client_channel.recv() => {
                if data.is_empty() {
                    info!("Closing UDP peer {} by request", t_inf);
                    with_quit_cleanup(&mut from_client_channel, &socket, peer, min_delay).await;
                    let mut channels_guard = server_channels.write().await;
                    match channels_guard.remove(&transfer) {
                        Some(_) => info!(
                            "Removed server channel for transfer {} in {}",
                            t_inf, service_name
                        ),
                        None => warn!(
                            "Server channel for transfer {} was already gone before remove in {}",
                            t_inf, service_name
                        ),
                    }
                    remove_route(&transfer).await;
                    break;
                }
                if let Err(err) = socket.send_to(data.as_ref(), peer).await {
                    error!("Failed to send UDP data to {} in {}: {}", t_inf, service_name, err);
                    break;
                } else {
                    in_bytes += data.len();
                    debug!("Sent {} bytes to UDP peer {} in {}", data.len(), t_inf, service_name);
                }
            },
            // Idle timeout: peer is not active anymore
            _ = sleep(idle_limit) => {
                warn!("Idle timeout for UDP peer {} ({})", peer, part_uuid(&transfer));
                let mut channels_guard = server_channels.write().await;
                match channels_guard.remove(&transfer) {
                    Some(_) => info!(
                        "Removed server channel for transfer {} in {}",
                        t_inf, service_name
                    ),
                    None => warn!(
                        "Server channel for transfer {} was already gone before remove in {}",
                        t_inf, service_name
                    ),
                }
                remove_route(&transfer).await;
                break;
            },
            // Periodic traffic statistics update (the immediate first tick is a no-op while counters are zero).
            _ = stat_tick.tick() => {
                if in_bytes > 0 {
                    let key = format!("udp-peer-{}", t_inf);
                    update_traffic_stats(&key, in_bytes, 0, 0).await;
                    in_bytes = 0;
                }
            }
        }
    }

    if in_bytes > 0 {
        let key = format!("udp-peer-{}", t_inf);
        update_traffic_stats(&key, in_bytes, 0, 0).await;
    }
    info!("Stopping UDP handler for {} in {}", t_inf, service_name);
}

async fn with_quit_cleanup(
    from_client_channel: &mut mpsc::Receiver<(Uuid, Bytes)>,
    socket: &Arc<UdpSocket>,
    peer: SocketAddr,
    min_delay: Duration,
) {
    loop {
        tokio::select! {
            _ = sleep(min_delay) => break,
            Some((_, data)) = from_client_channel.recv() => {
                if data.is_empty() {
                    continue;
                }
                if let Err(err) = socket.send_to(data.as_ref(), peer).await {
                    error!("Failed to send UDP quit drain data to {}: {}", peer, err);
                }
            },
        }
    }
}


pub async fn run_service<F>(settings: Settings, check_running: F) -> Result<(), Box<dyn std::error::Error>>
where
    F: Fn() -> bool + Send + 'static,
{
    if settings.tcp_sockets.is_empty() && settings.udp_sockets.is_empty() {
        warn!("No sockets configured; stopping client service");
        return Ok(());
    }

    let mut tasks: JoinSet<TaskResultEnum> = JoinSet::new();
    let (service_in_channel_size, _) = settings.channel_size();
    set_channel_size(service_in_channel_size).await;

    // Periodic stale route cleanup.
    let cleanup_interval = settings.route_cleanup_interval();
    tokio::spawn(async move {
        run_cleanup(cleanup_interval).await;
    });

    udp_client_processing(&settings, &mut tasks).await;
    tcp_client_processing(&settings, &mut tasks).await;

    // Stop watcher: ends service when check_running() becomes false.
    let check_delay = settings.service_delay();
    tasks.spawn(async move {
        loop {
            sleep(check_delay).await;
            if !check_running() {
                break;
            }
        }
        TaskResultEnum::StopService
    });

    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(val) => {
                if val == TaskResultEnum::WorkerDone {
                    info!("Task completed");
                } else {
                    warn!("Terminating all connections");
                    tasks.abort_all();
                }
            },
            Err(err) => {
                if err.is_cancelled() {
                    continue;
                }
                error!("Task failed: {}", err);
            }
        }
    }
    info!("Client service stopped");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_settings_empty_sockets() {
        let settings = crate::settings::create_settings("127.0.0.1", 8883, "", "", "", false, "");
        assert!(settings.tcp_sockets.is_empty());
        assert!(settings.udp_sockets.is_empty());
    }

    #[tokio::test]
    async fn test_run_service_with_tcp_socket() {
        let tcp = "svc1:127.0.0.1:9001";
        let settings = crate::settings::create_settings("127.0.0.1", 8883, "", tcp, "", false, "");
        assert_eq!(settings.tcp_sockets.len(), 1);

        static STOP: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
        std::thread::spawn(|| {
            std::thread::sleep(std::time::Duration::from_millis(300));
            STOP.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        let res = run_service(settings, || STOP.load(std::sync::atomic::Ordering::SeqCst)).await;
        assert!(res.is_ok());
    }
}
