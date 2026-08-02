use bytes::Bytes;
use log::{info, error, warn};
use tokio::sync::mpsc;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::task::JoinSet;
use tokio::time::sleep;
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use socket2::SockRef;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;
use crate::stat::{add_connection, lost_connection, update_metric, update_traffic_stats};
use crate::data::{DataHandler, DataHandlerSettings};
use crate::common::{OUT_TTL, Settings, LoadingParams, code_name, fast_name, part_uuid};

#[derive(PartialEq)]
enum TaskResultEnum {
    StopService,
    WorkerDone,
}

/// Handles data processing for a specific server connection.
/// Manages reading from the server, writing to the server, and forwarding data to clients.
async fn server_data_processing(
    tcp_service: bool,
    transfer: Uuid,
    settings: Arc<Settings>,
    service_code: Uuid,
    service_name: String,
    data_handler: Arc<DataHandlerSettings>,
    to_client_channel: mpsc::Sender<(Uuid, Bytes)>,
    mut to_server_channel: mpsc::Receiver<(Uuid, Bytes)>,
) {
    let server_host = settings.server_host.clone();
    let wait_before_close_time = settings.collect_message_timeout(true);
    let server_port= settings.server_port.clone();

    let mut size_buffer = vec![0u8; 4];
    let serv = format!("{} ({})", service_name, part_uuid(&service_code));
    let idle_limit = if tcp_service {settings.idle_tcp_limit} else {settings.idle_udp_limit};
    let (mut in_bytes, mut out_bytes, mut error_count) = (0, 0, 0);
    let stat_key = format!("out-total-{}", service_name);
    let buffer_size_limit = settings.buffer_size * 3;
    let stat_save_iter = settings.stat_save_iter;
    let service_conn_name = format!("to-server-{}", service_name);
    let min_delay = settings.service_delay();
    let s_buffer_size = settings.socket_recv_buffer_size();
    let verbose_log = settings.verbose;
    let mut init_try = true; 
    let mut skip_reconnect = true;
    let mut reconnection_count = 0;
    let mut reconnect_delay = settings.reconnect_delay(0);
    let mut get_size_problems = 0;

    loop {
        // keep connection loop
        let (mut reader, mut writer) = match TcpStream::connect((server_host.clone(), server_port)).await {
            Ok(stream) => {
                init_try = false;
                if stream.set_ttl(OUT_TTL).is_err() {
                    warn!("Failed to set TTL, current TTL is {}", stream.ttl().unwrap_or(0));
                }
                stream.set_nodelay(true).unwrap();
                let socket_ref = SockRef::from(&stream);
                if s_buffer_size > 0 {
                    socket_ref.set_recv_buffer_size(s_buffer_size).unwrap();
                }
                if !skip_reconnect {
                    get_size_problems = 0;
                    reconnection_count += 1;
                    reconnect_delay = settings.reconnect_delay(reconnection_count);
                    update_metric(&format!("{}-reconnection", service_name), reconnection_count).await;
                    info!("Reconnection for {}", serv);
                    match to_client_channel.send((Uuid::nil(), Bytes::new())).await {
                        Ok(_) => {},
                        Err(err) => {
                            error!("In reconnection case failed to send data to client channel for {}: {}", serv, err);
                        }
                    }
                }
                tokio::io::split(stream)
            },
            Err(err) => {
                error!("Failed to connect to TCP main server {}:{} : {}", server_host, server_port, err);
                if init_try || skip_reconnect {
                    sleep(wait_before_close_time).await;
                    std::process::exit(1);
                } else {
                    sleep(reconnect_delay).await;
                    continue;
                }
            }
        };
        add_connection(&service_conn_name).await;
        let mut without_attempt = false;
        loop {
            tokio::select! {
                read_result = reader.read(&mut size_buffer) => {
                    match read_result {
                        Ok(0) => {
                            info!("Connection closed by peer for {}", serv);
                            skip_reconnect = tcp_service;
                            break;
                        },
                        Ok(n) => {
                            if n != 4 {
                                warn!("Server stream got wrong size block: {}", n);
                                continue;
                            }
                            let size_bytes = &size_buffer[..n];
                            let expect_data_size = u32::from_le_bytes(size_bytes.try_into().unwrap()) as usize;
                            if expect_data_size > buffer_size_limit {
                                if verbose_log {
                                    info!(
                                        "Unexpectedly large data buffer size: {} bytes. Protocol limit is {} bytes.",
                                        expect_data_size,
                                        buffer_size_limit
                                    );
                                }
                                get_size_problems += 1;
                                continue;
                            }
                            let mut data = vec![0u8; expect_data_size];
                            match reader.read_exact(&mut data).await {
                                Ok(_) => {
                                    if verbose_log {
                                        info!("Server sent {} bytes", expect_data_size);
                                    }
                                },
                                Err(err) => {
                                    skip_reconnect = tcp_service;
                                    error_count += 1;
                                    error!("Failed to read data from server for {}: {}", serv, err);
                                    break;
                                }
                            }
                            match data_handler.load_data_message(&data) {
                                Ok(msg) => {
                                    if without_attempt {
                                        break;
                                    }
                                    let client_data = if msg.x {
                                        without_attempt = true;
                                        Bytes::new()
                                    } else {
                                        Bytes::from(msg.d)
                                    };
                                    if verbose_log {
                                        info!("Forwarding {} bytes to client channel", client_data.len());
                                    }
                                    match to_client_channel.send((msg.t, client_data)).await {
                                        Ok(_) => {
                                            in_bytes = n;
                                        },
                                        Err(err) => {
                                            error_count += 1;
                                            error!("Failed to send data to client channel for {}: {}", serv, err);
                                            break;
                                        }
                                    }
                                    if msg.x {
                                        info!("Connection closed by client request for {}", serv);
                                        if !skip_reconnect {
                                            sleep(min_delay).await;
                                        }
                                        break;
                                    }
                                },
                                Err(err) => {
                                    error_count += 1;
                                    skip_reconnect = tcp_service;
                                    error!("Failed to parse data message for {}: {}", serv, err);
                                    break;
                                }
                            }
                        },
                        Err(err) => {
                            error_count += 1;
                            skip_reconnect = tcp_service;
                            error!("Failed to read from TCP stream for {}: {}", serv, err);
                            break;
                        }
                    }
                },
                Some((client_transfer, msg_data)) = to_server_channel.recv() => {
                    let t_id = if client_transfer.is_nil() {&transfer} else {&client_transfer};
                    let msg = if msg_data.is_empty() {
                        without_attempt = true;
                        data_handler.make_quit_message(&service_code, t_id)
                    } else {
                        data_handler.make_data_message(&msg_data, &service_code, t_id)
                    };
                    let (s_part, d_part) = msg.dump(true);
                    if let Err(err) = writer.write_all(&s_part).await {
                        error_count += 1;
                        skip_reconnect = tcp_service;
                        error!("Failed to write to TCP stream for {}: {}", serv, err);
                        break;
                    } else if let Err(err) = writer.write_all(&d_part).await {
                        error_count += 1;
                        skip_reconnect = tcp_service;
                        error!("Failed to write to TCP stream for {}: {}", serv, err);
                        break;
                    } else {
                        out_bytes += msg.d.len();
                    }
                },
                _ = sleep(idle_limit) => {
                    if !tcp_service {
                        continue;
                    }
                    warn!("Connection closed due to idle timeout for {}", serv);
                    let msg = data_handler.make_quit_message(&service_code, &transfer);
                    let (s_part, d_part) = msg.dump(true);
                    if let Err(err) = writer.write_all(&s_part).await {
                        error_count += 1;
                        skip_reconnect = tcp_service;
                        error!("Failed to write quit message to TCP stream for {}: {}", serv, err);
                    } else if let Err(err) = writer.write_all(&d_part).await {
                        error_count += 1;
                        skip_reconnect = tcp_service;
                        error!("Failed to write quit message to TCP stream for {}: {}", serv, err);
                    } else {
                        out_bytes += msg.d.len();
                    }
                    break;
                },
                _ = sleep(stat_save_iter) => {
                    if get_size_problems > 0 {
                        update_metric(&format!("{}-wrong-size-block", stat_key), get_size_problems).await;
                    }
                    if in_bytes + out_bytes + error_count > 0 {
                        update_traffic_stats(&stat_key, in_bytes, out_bytes, error_count).await;
                        (in_bytes, out_bytes, error_count) = (0, 0, 0);
                    }
                }
            }
        }
        if skip_reconnect {
            // Post-cleanup phase: send remaining payload only
            loop {
                if without_attempt {
                    break;
                }

                tokio::select! {
                    read_result = reader.read(&mut size_buffer) => {
                        match read_result {
                            Ok(0) => {
                                info!("Connection closed by peer during cleanup for {}", serv);
                                break;
                            },
                            Ok(n) => {
                                if n != 4 {
                                    warn!("Server stream got wrong size block: {}", n);
                                    break;
                                }
                                let size_bytes = &size_buffer[..n];
                                let expect_data_size = u32::from_le_bytes(size_bytes.try_into().unwrap()) as usize;
                                if expect_data_size > buffer_size_limit {
                                    error!(
                                        "Unexpectedly large data buffer size during cleanup: {} bytes. Protocol limit is {} bytes.",
                                        expect_data_size,
                                        buffer_size_limit
                                    );
                                    break;
                                }
                                let mut data = vec![0u8; expect_data_size];
                                match reader.read_exact(&mut data).await {
                                    Ok(_) => {},
                                    Err(err) => {
                                        error_count += 1;
                                        error!("Failed to read data from server during cleanup for {}: {}", serv, err);
                                        break;
                                    }
                                }
                                match data_handler.load_data_message(&data) {
                                    Ok(msg) => {
                                        if msg.x {
                                            break;
                                        }
                                        match to_client_channel.send((msg.t, Bytes::from(msg.d))).await {
                                            Ok(_) => {
                                                in_bytes = n;
                                            },
                                            Err(err) => {
                                                error_count += 1;
                                                error!("Failed to send data to client during cleanup for {}: {}", serv, err);
                                                break;
                                            }
                                        }
                                    },
                                    Err(err) => {
                                        error_count += 1;
                                        error!("Failed to parse data message during cleanup for {}: {}", serv, err);
                                    }
                                }
                            },
                            Err(err) => {
                                error_count += 1;
                                error!("Failed to read from TCP stream during cleanup for {}: {}", serv, err);
                                break;
                            }
                        }
                    },
                    Some((client_transfer, msg_data)) = to_server_channel.recv() => {
                        if msg_data.is_empty() {
                            break;
                        }
                        let t_id = if client_transfer.is_nil() {&transfer} else {&client_transfer};
                        let msg = data_handler.make_data_message(&msg_data, &service_code, t_id);
                        let (s_part, d_part) = msg.dump(true);
                        if let Err(err) = writer.write_all(&s_part).await {
                            error_count += 1;
                            error!("Failed to write to TCP stream during cleanup for {}: {}", serv, err);
                            break;
                        } else if let Err(err) = writer.write_all(&d_part).await {
                            error_count += 1;
                            error!("Failed to write to TCP stream during cleanup for {}: {}", serv, err);
                            break;
                        } else {
                            out_bytes += msg.d.len();
                        }
                    },
                    _ = sleep(wait_before_close_time) => {
                        warn!("Connection closed due to idle timeout during cleanup for {}", serv);
                        break;
                    }
                }
            }
            if in_bytes + out_bytes + error_count > 0 {
                update_traffic_stats(&stat_key, in_bytes, out_bytes, error_count).await;
            }
            break;
        }
        lost_connection(&service_conn_name).await;
    }
}

/// Handles TCP connection for a client, managing data transfer and connection state.
async fn handle_tcp_connection(
    transfer: Uuid,
    settings: Arc<Settings>,
    mut reader: ReadHalf<TcpStream>,
    mut writer: WriteHalf<TcpStream>,
    ip: String,
    service: (String, Uuid),
    to_server_channel: mpsc::Sender<(Uuid, Bytes)>,
    mut from_client_channel: mpsc::Receiver<(Uuid, Bytes)>,
) {
    let verbose_log = settings.verbose;
    let mut buf = vec![0; settings.buffer_size];
    let idle_limit = settings.idle_tcp_limit;
    let wait_before_close_time = settings.collect_message_timeout(true);
    let min_delay = settings.service_delay();
    let (s_name, s_code) = service; 
    let stat_key = format!("in-{}-{}", s_name, ip);
    let service_name = format!("{} ({})", s_name, part_uuid(&s_code));
    let stat_save_iter = settings.stat_save_iter;
    let mut with_quit = true;
    let t_inf = part_uuid(&transfer);

    add_connection(&stat_key).await;
    info!("New connection {} in {} from {}", transfer, service_name, ip);
    let (mut in_bytes, mut out_bytes, mut error_count) = (0, 0, 0);

    loop {
        tokio::select! {
            // Read from client connection
            n = reader.read(&mut buf) => {
                let n = match n {
                    Ok(n) => n,
                    Err(err) => {
                        error_count += 1;
                        error!("Failed to read from client {} in {}: {}", ip, service_name, err);
                        break;
                    }
                };
                if n < 1 {
                    if verbose_log {
                        info!("Closing TCP connection for client {} in {} {}", ip, t_inf, service_name);
                    }
                    break;
                }
                out_bytes = n;
                if to_server_channel.send((Uuid::nil(), Bytes::from(buf[..n].to_vec()))).await.is_err() {
                    warn!("Failed to forward data to server channel for {} in {}", t_inf, service_name);
                    error_count += 1;
                    break;
                } else {
                    if verbose_log {
                        info!("Forwarded {} bytes to server channel for {} in {}", n, t_inf, service_name);
                    }
                }
            },
            // Write to client connection
            Some((_, data)) = from_client_channel.recv() => {
                if data.is_empty() {
                    info!("Closing connection {} client {} by request", ip, t_inf);
                    with_quit = false;
                    break;
                }
                if let Err(err) = writer.write_all(&data).await {
                    error_count += 1;
                    error!("Failed to write to client {} in {}: {}", t_inf, service_name, err);
                    break;
                } else {
                    in_bytes = data.len();
                    if verbose_log {
                        info!("Sent {} bytes to client {} in {}", data.len(), t_inf, service_name);
                    }
                }
            },
            // Idle timeout
            _ = sleep(idle_limit) => {
                warn!("Idle timeout for TCP connection {} client {}", t_inf, ip);
                break;
            },
            _ = sleep(stat_save_iter) => {
                if in_bytes + out_bytes + error_count > 0 {
                    update_traffic_stats(&stat_key, in_bytes, out_bytes, error_count).await;
                    (in_bytes, out_bytes, error_count) = (0, 0, 0);
                }
            }
        }
    }

    // Final connection cleanup and draining remaining data
    lost_connection(&stat_key).await;
    info!("Stopping connection handler for {} in {}", t_inf, service_name);

    loop {
        tokio::select! {
            _ = sleep(wait_before_close_time) => {
                if with_quit {
                    if to_server_channel.send((Uuid::nil(), Bytes::new())).await.is_err() {
                        warn!("Failed to send termination signal to channel for {} in {}", t_inf, service_name);
                    } else {
                        info!("Sent termination signal to channel for {} in {}", t_inf, service_name);
                    }
                }
                break;
            },
            Some((_, data)) = from_client_channel.recv() => {
                if data.is_empty() {
                    sleep(min_delay).await;
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
}

/// Handles data forwarding for a specific UDP transfer session.
async fn handle_udp_connection(
    transfer: Uuid,
    settings: Arc<Settings>,
    socket: &UdpSocket,
    service: (String, Uuid),
    to_server_channel: mpsc::Sender<(Uuid, Bytes)>,
    mut from_client_channel: mpsc::Receiver<(Uuid, Bytes)>,
) {
    let verbose_log = settings.verbose;
    let mut buf = vec![0; settings.buffer_size];
    let stat_save_iter = settings.idle_udp_limit;
    let min_delay = settings.service_delay();
    let (s_name, s_code) = service; 
    let service_name = format!("{} ({})", s_name, part_uuid(&s_code));
    let (mut in_bytes, mut out_bytes, mut error_count) = (0, 0, 0);
    let mut stat_key = String::new();
    let mut transfer_peer = HashMap::new();

    loop {
        tokio::select! {
            res = socket.recv_from(&mut buf) => {
                let (n, peer) = match res {
                    Ok((n, peer)) => (n, peer),
                    Err(err) => {
                        error!("UDP receive error for service {}: {}", service_name, err);
                        sleep(min_delay).await;
                        continue;
                    }
                };
                if n < 1 {
                    if verbose_log {
                        info!("Closing UDP connection for peer {}", peer);
                    }
                    sleep(min_delay).await;
                    continue;
                }
                let transfer_id = code_name(&format!("{}-{}", transfer, peer));
                if !transfer_peer.contains_key(&transfer_id) {
                    let current_ip = peer.ip().to_string();
                    stat_key = format!("in-{}-{}", s_name, current_ip);
                    add_connection(&stat_key).await;
                    info!("New connection {} for service {} from {}", transfer_id, service_name, peer);
                    transfer_peer.insert(transfer_id.clone(), peer);
                }
                out_bytes += n;
                if to_server_channel.send((transfer_id, Bytes::from(buf[..n].to_vec()))).await.is_err() {
                    warn!("Failed to send data to channel for UDP client {} in service {}", part_uuid(&transfer), service_name);
                    error_count += 1;
                    break;
                } else {
                    if verbose_log {
                        info!("Sent {} bytes to channel for client {} in service {}", n, transfer, service_name);
                    }
                }
            },
            Some((transfer_id, data)) = from_client_channel.recv() => {
                if transfer_id.is_nil() {
                    sleep(min_delay).await;
                    info!("Peer list is cleared for {} after reconnection", transfer_id);
                    continue;
                }
                if data.is_empty() {
                    info!("Closing UDP transfer for client {} by request", transfer_id);
                    continue;
                }
                if let Some(peer) = transfer_peer.get(&transfer_id) {
                    let current_ip = peer.ip().to_string();
                    stat_key = format!("in-{}-{}", s_name, current_ip);                    
                    in_bytes += match socket.send_to(&data, peer).await {
                        Ok(sent_n) => sent_n,
                        Err(err) => {
                            error!("UDP send error for client {} to {}: {}", transfer_id, current_ip, err);
                            error_count += 1;
                            0
                        }
                    };
                } else {
                    warn!("Unknown peer (or waiting) for UDP transfer {} in service {}", transfer_id, service_name);
                }
            },
            _ = sleep(stat_save_iter) => {
                if in_bytes + out_bytes + error_count > 0 && !stat_key.is_empty() {
                    update_traffic_stats(&stat_key, in_bytes, out_bytes, error_count).await;
                    (in_bytes, out_bytes, error_count) = (0, 0, 0);
                }
            },
        }
    }
    if in_bytes + out_bytes + error_count > 0 && !stat_key.is_empty() {
        update_traffic_stats(&stat_key, in_bytes, out_bytes, error_count).await;
    }
}

/// Processes incoming TCP client connections and spawns tasks for data handling.
async fn tcp_client_processing(settings: &Settings, tasks: &mut JoinSet<TaskResultEnum>) -> TaskResultEnum {
    let tcp_keys: Vec<_> = settings.tcp_sockets.keys().cloned().collect();
    let data_handler = Arc::new(DataHandlerSettings::new(settings));
    for serv_name in settings.udp_sockets.keys() {
        if tcp_keys.contains(serv_name) {
            error!(
                "Configuration conflict: service '{}' is defined in both UDP and TCP sockets",
                serv_name
            );
            return TaskResultEnum::WorkerDone;
        }
    }

    for (service_name, ip_port_map) in settings.tcp_sockets.iter() {
        let Some((with_ip, with_port)) = ip_port_map.iter().next() else {
            error!("No IP-Port mapping found for server {}", service_name);
            continue;
        };
        let (service_in_channel_size, service_out_channel_size) = settings.channel_size();
        let service_code = code_name(&service_name);
        let ip = with_ip.clone();
        let port = with_port.clone();
        let service_name = service_name.clone();
        let local_settings_arc = Arc::new(settings.clone());
        let data_handler = data_handler.clone();

        tasks.spawn(async move {
            let listener = match TcpListener::bind((ip, port)).await {
                Ok(listener) => listener,
                Err(err) => {
                    error!("Failed to bind TCP listener to {}:{}: {}", ip, port, err);
                    update_metric("bind-listener-error", 1).await;
                    return TaskResultEnum::WorkerDone;
                }
            };

            info!("Listening on tcp://{}:{} service {} ({})", ip, port, service_name, service_code);
            let serv_name = service_name.clone();
            loop {
                let (stream, c_addr) = match listener.accept().await {
                    Ok(result) => result,
                    Err(err) => {
                        error!("Failed to accept TCP connection: {}", err);
                        break;
                    }
                };
                let c_ip = c_addr.ip().to_string();
                let transfer = fast_name();
                let transfer_id = transfer.clone();
                let serv_code = service_code.clone();
                let s_name = serv_name.clone();
                let local_settings = local_settings_arc.clone();
                let client_settings = local_settings_arc.clone();
                let (client_tx, client_rx) = mpsc::channel(service_out_channel_size);
                let (server_tx, server_rx) = mpsc::channel(service_in_channel_size);
                let data_handler = data_handler.clone();

                tokio::spawn(async move {
                    server_data_processing(
                        true,
                        transfer_id,
                        local_settings,
                        serv_code,
                        s_name,
                        data_handler,
                        client_tx,
                        server_rx,
                    ).await;
                });

                let serv_code = service_code.clone();
                let s_name = serv_name.clone();
                
                tokio::spawn(async move {
                    let (reader, writer) = tokio::io::split(stream);
                    handle_tcp_connection(
                        transfer,
                        client_settings,
                        reader,
                        writer,
                        c_ip,
                        (s_name, serv_code),
                        server_tx,
                        client_rx,
                    ).await;
                });
            }
            TaskResultEnum::WorkerDone
        });
    }
    TaskResultEnum::WorkerDone
}

/// Processes UDP client connections based on settings and spawns tasks.
async fn udp_client_processing(settings: &Settings, tasks: &mut JoinSet<TaskResultEnum>) -> TaskResultEnum {
    let data_handler = Arc::new(DataHandlerSettings::new(settings));

    for (service_name, ip_port_map) in settings.udp_sockets.iter() {
        let Some((with_ip, with_port)) = ip_port_map.iter().next() else {
            error!("No IP-Port mapping found for UDP server '{}'", service_name);
            continue;
        };
        let (service_in_channel_size, service_out_channel_size) = settings.channel_size();
        let service_code = code_name(&service_name);
        let ip = with_ip.clone();
        let port = with_port.clone();
        let service_name = service_name.clone();
        let local_settings_arc = Arc::new(settings.clone());
        let data_handler = data_handler.clone();

        tasks.spawn(async move {
            let server_socket = match UdpSocket::bind((ip, port)).await {
                Ok(result) => {
                    info!("Listening on udp://{}:{} service {} ({})", ip, port, service_name, service_code);
                    result
                },
                Err(err) => {
                    error!("Failed to bind UDP socket for service '{}': {}", service_name, err);
                    update_metric("bind-listener-error", 1).await;
                    return TaskResultEnum::WorkerDone;
                }
            };

            let transfer = fast_name();
            let transfer_id = transfer.clone();
            let serv_code = service_code.clone();
            let s_name = service_name.clone();
            let client_settings = local_settings_arc.clone();
            let (client_tx, client_rx) = mpsc::channel(service_out_channel_size);
            let (server_tx, server_rx) = mpsc::channel(service_in_channel_size);
            let data_handler = data_handler.clone();

            tokio::spawn(async move {
                server_data_processing(
                    false,
                    transfer_id,
                    local_settings_arc,
                    serv_code,
                    s_name,
                    data_handler,
                    client_tx,
                    server_rx,
                ).await;
            });

            let serv_code = service_code.clone();
            handle_udp_connection(
                transfer,
                client_settings,
                &server_socket,
                (service_name, serv_code),
                server_tx,
                client_rx,
            ).await;
            TaskResultEnum::WorkerDone
        });
    }
    TaskResultEnum::WorkerDone
}

/// Runs both TCP and UDP client processing tasks.
pub async fn run_service<F>(settings: Settings, check_running: F) -> Result<(), Box<dyn std::error::Error>>
where
    F: Fn() -> bool + Send + 'static, 
{
    let mut tasks: JoinSet<TaskResultEnum> = JoinSet::new();
    if !settings.tcp_sockets.is_empty() {
        tcp_client_processing(&settings, &mut tasks).await;
    }
    if !settings.udp_sockets.is_empty() {
        udp_client_processing(&settings, &mut tasks).await;
    }
    let chek_delay = settings.service_delay();
    tasks.spawn(async move {
        let mut wait = true;
        while wait {
            sleep(chek_delay).await;
            wait = check_running();
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
            }
            Err(err) => {
                if settings.verbose {
                    error!("Task failed: {}", err);
                }
                count_err += 1;
            }
        }
    }
    info!("Terminated tasks: {}", count_err);
    Ok(())
}
