use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use uuid::Uuid;

use crate::data::{DataHandler, DataHandlerSettings};
use crate::route::{exists, send_data, add_route, remove_route, set_channel_size, run_cleanup};
use crate::settings::{LoadingParams, Settings, code_name, fast_name, part_uuid, TaskResultEnum};
use crate::stat::{add_connection, update_metric, update_traffic_stats, lost_connection, periodic_dump};

static CLIENT_RUNNING: AtomicBool = AtomicBool::new(false);

pub fn is_running() -> bool {
    CLIENT_RUNNING.load(Ordering::SeqCst)
}

pub fn stop() {
    let changed = CLIENT_RUNNING.swap(false, Ordering::SeqCst);
    if changed {
        info!("Client stop requested");
    }
}

async fn server_connection<T: LoadingParams + Send + 'static>(
    params: T,
    service_name: String,
    connection_info: String,
    addr: String,
    data_handler: Arc<DataHandlerSettings>,
    mut to_server_channel: mpsc::Receiver<Bytes>,
) {
    let mut reconnect_attempts: usize = 0;
    let mut format_error_count: usize = 0;
    let mut route_notfound_count: usize = 0;
    let mut transfer_out: usize = 0;
    let mut transfer_in: usize = 0;
    let mut transfer_error: usize = 0;
    let update_stat_delay = params.reconnect_delay(0) * 2;
    let stat_key = format!("out-connection-{}", connection_info);
    let stat_key_all = format!("out-connection-{}", service_name);
    let metric_no_route_key = format!("no-route-error-{}", connection_info);
    let metric_format_err_key = format!("format-error-{}", connection_info);
    let mut done = false;
    let conn_key = format!("to-server-out-{}", connection_info);
    add_connection(&conn_key).await;

    loop {
        match TcpStream::connect(&addr).await {
            Ok(stream) => {
                info!("Connection for {} established", connection_info);
                let mut framed = Framed::new(stream, LengthDelimitedCodec::new());
                // processing in/out
                loop {
                    tokio::select! {
                        frame_opt = framed.next() => {
                            match frame_opt {
                                Some(Ok(frame)) => {
                                    let bytes_slice = frame.as_ref();
                                    let n = bytes_slice.len();
                                    reconnect_attempts = 0;
                                    match data_handler.load_data_message(bytes_slice) {
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
                                        }
                                         Err(err) => {
                                             error!("Wrong message format {} in connection: {}", err, connection_info);
                                             format_error_count += 1;
                                         }
                                    }
                                },
                                Some(Err(err)) => {
                                    error!("Read error {} in connection: {}", err, connection_info);
                                    break;
                                },
                                None => {
                                    info!("Connection closed for {} in connection: {}", addr, connection_info);
                                    done = true;
                                    break;
                                },
                            }
                        },
                        Some(data) = to_server_channel.recv() => {
                            let iter_n = data.len();
                            match framed.send(data).await {
                                Ok(_) => {
                                    transfer_out += iter_n;
                                },
                                  Err(err) => {
                                      error!("Send error {} in connection: {}", err, connection_info);
                                      transfer_error += 1;
                                      break;
                                  }
                              }
                          },
                        _ = sleep(update_stat_delay) => {
                            if transfer_in > 0 || transfer_out > 0 || transfer_error > 0 {
                                update_traffic_stats(&stat_key, transfer_in, transfer_out, transfer_error).await;
                                update_traffic_stats(&stat_key_all, transfer_in, transfer_out, transfer_error).await;
                            }
                            if route_notfound_count > 0 {
                                update_metric(&metric_no_route_key, route_notfound_count).await;
                            }
                            if format_error_count > 0 {
                                update_metric(&metric_format_err_key, format_error_count).await;
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
            update_traffic_stats(&stat_key, transfer_in, transfer_out, transfer_error).await;
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
            let delay = params.reconnect_delay(reconnect_attempts);
            warn!("Reconnection attempt {} in {} (after: {} ms)", connection_info, reconnect_attempts + 1, delay.as_millis());
            sleep(delay).await;
            reconnect_attempts += 1;
        }
    }
    lost_connection(&conn_key).await;
}

async fn tcp_client_processing(settings: &Settings, tasks: &mut JoinSet<TaskResultEnum>) {
    let tcp_keys: Vec<_> = settings.tcp_sockets.keys().cloned().collect();
    let data_handler = Arc::new(DataHandlerSettings::new(settings));
    for serv_name in settings.udp_sockets.keys() {
        if tcp_keys.contains(serv_name) {
            error!(
                "Configuration conflict: service '{}' is defined in both UDP and TCP sockets",
                serv_name
            );
            return;
        }
    }

    for (service_name, ip_port_map) in settings.tcp_sockets.iter() {
        let Some((with_ip, with_port)) = ip_port_map.iter().next() else {
            error!("No IP-Port mapping found for server {}", service_name);
            continue;
        };
        let service_code = code_name(&service_name);
        let ip = with_ip.clone();
        let port = with_port.clone();
        let service_name = service_name.clone();
        let settings = settings.clone();
        let (_, service_out_channel_size) = settings.channel_size();
        let addr = format!("{}:{}", settings.server_host, settings.server_port);

        let data_handler = data_handler.clone();
        tasks.spawn(async move {
            let idle_limit = settings.idle_tcp_limit;
            let listener = match TcpListener::bind((ip, port)).await {
                Ok(listener) => listener,
                Err(err) => {
                    error!("Failed to bind TCP listener to {}:{}: {}", ip, port, err);
                    return TaskResultEnum::WrongSettings;
                }
            };
            info!("Listening on tcp://{}:{} service {} ({})", ip, port, service_name, service_code);
            loop {
                // service client connection
                let (stream, c_addr) = match listener.accept().await {
                    Ok(result) => result,
                    Err(err) => {
                        error!("Failed to accept TCP connection: {}", err);
                        break;
                    }
                };
                let transfer = fast_name();
                let s_code = service_code.clone();
                let serv_name = service_name.clone();
                let stat_key = format!("in-{}-{}", serv_name, ip);
                let service_name_display = format!("{} ({})", serv_name, part_uuid(&s_code));
                let stat_save_iter = settings.stat_save_iter;
                let buffer_size = settings.buffer_size;
                let wait_before_close_time = settings.collect_message_timeout(true);
                let data_handler_conn = data_handler.clone();
                let data_handler_out = data_handler.clone();
                let (serv_tx, serv_rx) = mpsc::channel(service_out_channel_size);
                let to_server_channel = serv_tx.clone();
                let server_addr= addr.clone();
                let from_client_channel = add_route(&transfer).await;
                info!("New connection {} in {} from {}", transfer, service_name_display, c_addr);
                let conn_info = c_addr.ip().to_string();
                let conn_settings = settings.clone();
                tokio::spawn(async move {
                    server_connection(
                        conn_settings,
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
                        to_server_channel,
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
    idle_limit: Duration,
    stat_save_iter: Duration,
    wait_before_close_time: Duration,
) {
    add_connection(&stat_key).await;
    let t_inf = part_uuid(&transfer);
    let mut with_quit = false;
    let mut buf = vec![0; buffer_size];
    let (mut reader, mut writer) = tokio::io::split(stream);
    let (mut in_bytes, mut out_bytes, mut error_count) = (0, 0, 0);
    // data handler
    loop {
        tokio::select! {
            // Read from client connection
            n = reader.read(&mut buf) => {
                let n = match n {
                    Ok(n) => n,
                    Err(err) => {
                        error_count += 1;
                        error!("Failed to read from client {} in {}: {}", c_addr, service_name, err);
                        break;
                    }
                };
                if n < 1 {
                    debug!("Closing TCP connection for client {} in {} {}", c_addr, t_inf, service_name);
                    break;
                }
                out_bytes = n;
                let data = data_handler.make_data_message(&buf[..n], &service_code, &transfer);
                if let Err(err) = to_server_channel.send(data).await {
                    warn!("Failed to send data to server channel {} in {}: {}", t_inf, service_name, err);
                    break;
                } else {
                    with_quit = true;
                }
            },
            // Write to client connection
            Some((_, data)) = from_client_channel.recv() => {
                if data.is_empty() {
                    info!("Closing connection {} client {} by request", c_addr, t_inf);
                    with_quit = false;
                    break;
                }
                if let Err(err) = writer.write_all(&data).await {
                    error_count += 1;
                    error!("Failed to write to client {} in {}: {}", t_inf, service_name, err);
                    break;
                } else {
                    in_bytes += data.len();
                    debug!("Sent {} bytes to client {} in {}", data.len(), t_inf, service_name);
                }
            },
            // Idle timeout
            _ = sleep(idle_limit) => {
                warn!("Idle timeout for TCP connection {} client {}", t_inf, c_addr);
                break;
            },
            // Periodic traffic statistics update
            _ = sleep(stat_save_iter) => {
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
    // Final connection cleanup and draining remaining data before close
    loop {
        tokio::select! {
            _ = sleep(wait_before_close_time) => {
                if with_quit {
                    let data = data_handler.make_quit_message(&service_code, &transfer);
                    if let Err(err) = to_server_channel.send(data).await {
                        warn!("Failed to send quit signal to server channel {} in {}: {}", t_inf, service_name, err);
                    }
                }
                break;
            },
            Some((_, data)) = from_client_channel.recv() => {
                if data.is_empty() {
                    with_quit = false;
                    continue;
                }
                if let Err(err) = writer.write_all(&data).await {
                    error!("Failed to write to client {} in {}: {}", t_inf, service_name, err);
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
        let service_code = code_name(&service_name);
        let ip = with_ip.clone();
        let port = with_port.clone();
        let service_name = service_name.clone();
        let data_handler = data_handler.clone();
        let settings_local = settings.clone();
        let addr = format!("{}:{}", settings.server_host, settings.server_port);
        tasks.spawn(async move {
            let buffer_size = settings_local.buffer_size;
            let min_delay = settings_local.service_delay();
            let idle_limit = settings_local.idle_udp_limit;
            let stat_save_iter = settings_local.stat_save_iter;
            let wait_before_close_time = settings_local.collect_message_timeout(true);
            let server_socket = match UdpSocket::bind((ip, port)).await {
                Ok(result) => {
                    info!("Listening on udp://{}:{} service {} ({})", ip, port, service_name, service_code);
                    result
                },
                Err(err) => {
                    error!("Failed to bind UDP socket for service '{}': {}", service_name, err);
                    return TaskResultEnum::WrongSettings;
                }
            };
            let socket_arc = Arc::new(server_socket);
            let serv_code = service_code.clone();
            let client_name = settings_local.client_name;
            let service_display = format!("'{}' ({})", service_name, part_uuid(&serv_code));
            let stat_key = format!("in-{}-{}", service_name, ip);
            let mut buf = vec![0; buffer_size];
            let server_channels = Arc::new(tokio::sync::RwLock::new(HashMap::new()));
            let conn_settings = settings_local.clone();
            let (_, service_out_channel_size) = settings_local.channel_size();
            let mut out_bytes: usize = 0;
            loop {
                if !is_running() {
                    break;
                }
                tokio::select! {
                    recv_result = socket_arc.recv_from(&mut buf) => match recv_result {
                        Ok((n, peer)) => {
                            out_bytes += n;
                            if n < 1 {
                                debug!("Empty packet from UDP peer {}", peer);
                            } else {
                                let transfer = code_name(&format!("{}-{}-{}", serv_code, client_name, peer));
                                let data = data_handler.make_data_message(&buf[..n], &serv_code, &transfer);
                                if !exists(&transfer).await {
                                    add_connection(&stat_key).await;
                                    info!("New UDP peer {} in {}", part_uuid(&transfer), service_display);
                                    let from_client_channel = add_route(&transfer).await;
                                    let s_name = service_name.clone();
                                    let socket = Arc::clone(&socket_arc);
                                    let stat_key_peer = stat_key.clone();
                                    let server_addr = addr.clone();
                                    let conn_info = peer.ip().to_string();
                                    let service_display_peer = service_display.clone();
                                    let (serv_tx, serv_rx) = mpsc::channel(service_out_channel_size);
                                    server_channels.write().await.insert(transfer, serv_tx);
                                    let conn_settings_spawn = conn_settings.clone();
                                    let data_handler_spawn = data_handler.clone();
                                    tokio::spawn(async move {
                                        server_connection(
                                            conn_settings_spawn,
                                            s_name,
                                            conn_info,
                                            server_addr,
                                            data_handler_spawn,
                                            serv_rx,
                                        ).await;
                                    });

                                    let channels_clone = Arc::clone(&server_channels);
                                    tokio::spawn(async move {
                                        udp_peer_processing(
                                            peer,
                                            transfer,
                                            serv_code,
                                            service_display_peer,
                                            stat_key_peer,
                                            from_client_channel,
                                            socket,
                                            channels_clone,
                                            min_delay,
                                            idle_limit,
                                            stat_save_iter,
                                            wait_before_close_time,
                                        ).await;
                                    });
                                }
                                {
                                    // Read lock + cloned sender: the map is read on every packet, and sending needs no mutation.
                                    let tx_opt = server_channels.read().await.get(&transfer).cloned();
                                    match tx_opt {
                                        Some(tx) => {
                                            if let Err(err) = tx.send(data).await {
                                                warn!("Failed to send data to server channel {} in {}: {}", part_uuid(&transfer), service_display, err);
                                                server_channels.write().await.remove(&transfer);
                                            }
                                        },
                                        None => error!("No route for transfer {} in {}", part_uuid(&transfer), service_display),
                                    }
                                }
                            }
                        },
                        Err(err) => {
                            error!("UDP receive error for service {}: {}", service_name, err);
                            lost_connection(&stat_key).await;
                            sleep(min_delay).await;
                        },
                    },
                    // Periodic out-traffic statistics update
                    _ = sleep(stat_save_iter) => {
                        if out_bytes > 0 {
                            update_traffic_stats(&stat_key, 0, out_bytes, 0).await;
                            out_bytes = 0;
                        }
                    }
                }
            }
            if out_bytes > 0 {
                update_traffic_stats(&stat_key, 0, out_bytes, 0).await;
            }
            TaskResultEnum::WorkerDone
        });
    }
}

async fn udp_peer_processing(
    peer: SocketAddr,
    transfer: Uuid,
    service_code: Uuid,
    service_name: String,
    stat_key: String,
    mut from_client_channel: mpsc::Receiver<(Uuid, Bytes)>,
    socket: Arc<UdpSocket>,
    server_channels: Arc<tokio::sync::RwLock<HashMap<Uuid, mpsc::Sender<Bytes>>>>,
    min_delay: Duration,
    idle_limit: Duration,
    stat_save_iter: Duration,
    wait_before_close_time: Duration,
) {
    let t_inf = part_uuid(&transfer);
    let s_part = part_uuid(&service_code);
    let (mut in_bytes, mut error_count) = (0, 0);
    loop {
        tokio::select! {
            // Send data from server connection to peer
            Some((_, data)) = from_client_channel.recv() => {
                if data.is_empty() {
                    info!("Closing UDP peer {} by request", t_inf);
                    with_quit_cleanup(&stat_key, &mut from_client_channel, &socket, peer, min_delay, wait_before_close_time).await;
                    remove_route(&transfer).await;
                    break;
                }
                if let Err(err) = socket.send_to(data.as_ref(), peer).await {
                    error_count += 1;
                    error!("Failed to send UDP data to {} in {}: {}", t_inf, service_name, err);
                    break;
                } else {
                    in_bytes += data.len();
                    debug!("Sent {} bytes to UDP peer {} in {}", data.len(), t_inf, service_name);
                }
            },
            // Idle timeout: peer is not active anymore
            _ = sleep(idle_limit) => {
                warn!("Idle timeout for UDP peer {} ({})", peer, s_part);
                lost_connection(&stat_key).await;
                server_channels.write().await.remove(&transfer);
                remove_route(&transfer).await;
                break;
            },
            // Periodic traffic statistics update
            _ = sleep(stat_save_iter) => {
                if in_bytes + error_count > 0 {
                    update_traffic_stats(&stat_key, in_bytes, 0, error_count).await;
                    (in_bytes, error_count) = (0, 0);
                }
            }
        }
    }
    if in_bytes + error_count > 0 {
        update_traffic_stats(&stat_key, in_bytes, 0, error_count).await;
    }
    lost_connection(&stat_key).await;
    info!("Stopping UDP handler for {} in {} ({})", t_inf, service_name, s_part);
}

async fn with_quit_cleanup(
    stat_key: &str,
    from_client_channel: &mut mpsc::Receiver<(Uuid, Bytes)>,
    socket: &Arc<UdpSocket>,
    peer: SocketAddr,
    min_delay: Duration,
    wait_before_close_time: Duration,
) {
    loop {
        tokio::select! {
            _ = sleep(wait_before_close_time) => break,
            Some((_, data)) = from_client_channel.recv() => {
                if data.is_empty() {
                    sleep(min_delay).await;
                    continue;
                }
                if let Err(err) = socket.send_to(data.as_ref(), peer).await {
                    error!("Failed to send UDP quit drain data to {}: {}", peer, err);
                } else {
                    update_traffic_stats(stat_key, 1, 0, 0).await;
                }
            },
        }
    }
}

pub async fn run(settings: Settings) {
    if settings.tcp_sockets.is_empty() && settings.udp_sockets.is_empty() {
        std::process::exit(1);
    }
    CLIENT_RUNNING.store(true, Ordering::SeqCst);

    let mut tasks: JoinSet<TaskResultEnum> = JoinSet::new();
    let (service_in_channel_size, _) = settings.channel_size();
    set_channel_size(service_in_channel_size).await;

    // Periodic stale route cleanup.
    let cleanup_interval = settings.route_cleanup_interval();
    tokio::spawn(async move {
        run_cleanup(cleanup_interval).await;
    });
    udp_client_processing(&settings, &mut tasks).await;
    tcp_client_processing(&settings,  &mut tasks).await;
    // Periodic statistics dump to file with memory/uptime metrics.
    let stat_delay = settings.stat_delay;
    let stat_filepath = settings.stat_filepath.clone();
    tasks.spawn(async move {
        periodic_dump(&stat_filepath, stat_delay).await;
        TaskResultEnum::WorkerDone
    });

    // Stop watcher: returns StopService when stop() is called.
    let check_delay = settings.service_delay();
    tasks.spawn(async move {
        loop {
            sleep(check_delay).await;
            if !is_running() {
                break;
            }
        }
        TaskResultEnum::StopService
    });

    let mut count_err = 0;
    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(val) => {
                if val == TaskResultEnum::WorkerDone {
                    info!("Task completed");
                } else {
                    warn!("Termination all connections");
                    tasks.abort_all();
                }
            },
            Err(err) => {
                if err.is_cancelled() {
                    continue;
                }
                error!("Task failed: {}", err);
                count_err += 1;
            }
        }
    }
    CLIENT_RUNNING.store(false, Ordering::SeqCst);
    info!("Terminated tasks: {}", count_err);
}

#[cfg(test)]
mod tests {
    use super::*;
    use once_cell::sync::Lazy;
    use tokio::sync::Mutex;

    static TEST_MUTEX: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

    async fn test_guard() -> tokio::sync::MutexGuard<'static, ()> {
        TEST_MUTEX.lock().await
    }

    #[tokio::test]
    async fn test_client_is_running_and_stop() {
        let _g = test_guard().await;
        assert!(!is_running(), "fresh process flag must start as not running");
        CLIENT_RUNNING.store(true, Ordering::SeqCst);
        assert!(is_running());
        stop();
        assert!(!is_running());
    }
}
