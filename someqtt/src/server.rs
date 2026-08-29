use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use log::{debug, error, info, warn};
use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::str::FromStr;
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

use crate::data::{DataHandler, DataHandlerSettings, DataMessageError};
use crate::route::{exists, send_data, add_route, remove_route, set_channel_size, run_cleanup};
use crate::settings::{code_name, part_uuid, Settings, LoadingParams, OUT_TTL, TaskResultEnum};
use crate::stat::{add_connection, lost_connection, periodic_dump, update_metric, update_traffic_stats};

static SERVER_RUNNING: AtomicBool = AtomicBool::new(false);

pub fn is_running() -> bool {
    SERVER_RUNNING.load(Ordering::SeqCst)
}

pub fn stop() {
    let changed = SERVER_RUNNING.swap(false, Ordering::SeqCst);
    if changed {
        info!("Server stop requested");
    }
}

struct TargetConfig {
    service_code: Uuid,
    service_name: String,
    ip: IpAddr,
    port: u16,
    is_udp: bool,
    buffer_size: usize,
    idle_limit: Duration,
    collect_timeout: Duration,
    udp_bind_ip: IpAddr,
}

fn create_outbound_port(transfer: Uuid) -> u16 {
    let bytes = transfer.as_bytes();
    let four_bytes_val = ((bytes[0] as u32) << 24) | ((bytes[1] as u32) << 16) | ((bytes[2] as u32) << 8) | (bytes[3] as u32);
    let range_size = (u16::MAX as u32) - 256;
    let remainder = four_bytes_val % range_size;
    let shift: u16 = (bytes[4] % 32) as u16; // deterministic spread in [0..=31]
    ((remainder + 240) as u16).wrapping_add(shift)
}

fn udp_bind_ip(settings: &Settings) -> IpAddr {
    match SocketAddr::from_str(&settings.udp_bind_from) {
        Ok(addr) => addr.ip(),
        Err(_) => Ipv4Addr::UNSPECIFIED.into(),
    }
}

fn resolve_target(service_code: &Uuid, settings: &Settings) -> Option<TargetConfig> {
    let buffer_size = settings.buffer_size;
    let collect_timeout = settings.collect_message_timeout(true);
    let any_v4: IpAddr = Ipv4Addr::UNSPECIFIED.into();
    for (name, map) in settings.tcp_targets.iter() {
        if code_name(name) == *service_code {
            let Some((ip, port)) = map.iter().next() else { continue; };
            return Some(TargetConfig {
                service_code: *service_code,
                service_name: name.clone(),
                ip: *ip,
                port: *port,
                is_udp: false,
                buffer_size,
                idle_limit: settings.idle_tcp_limit,
                collect_timeout,
                udp_bind_ip: any_v4,
            });
        }
    }
    for (name, map) in settings.udp_targets.iter() {
        if code_name(name) == *service_code {
            let Some((ip, port)) = map.iter().next() else { continue; };
            return Some(TargetConfig {
                service_code: *service_code,
                service_name: name.clone(),
                ip: *ip,
                port: *port,
                is_udp: true,
                buffer_size,
                idle_limit: settings.idle_udp_limit,
                collect_timeout,
                udp_bind_ip: udp_bind_ip(settings),
            });
        }
    }
    None
}

async fn process_data_transfer_tcp(
    cfg: TargetConfig,
    transfer: Uuid,
    mut from_client_channel: mpsc::Receiver<(Uuid, Bytes)>,
    to_client_channel: mpsc::Sender<(Uuid, Uuid, Bytes)>,
) {
    let serv = format!("{} ({}) - {}:{}", cfg.service_name, part_uuid(&cfg.service_code), cfg.ip, cfg.port);
    let tcp_stream = match TcpStream::connect((cfg.ip, cfg.port)).await {
        Ok(stream) => stream,
        Err(err) => {
            error!("Failed to connect to TCP target '{}' in {}: {}", part_uuid(&transfer), serv, err);
            return;
        }
    };
    if let Err(e) = tcp_stream.set_ttl(OUT_TTL) {
        warn!("TTL set failed for {}: {}", serv, e);
    }
    let _ = tcp_stream.set_nodelay(true);
    let (mut reader, mut writer) = tokio::io::split(tcp_stream);
    let mut read_buffer = vec![0u8; cfg.buffer_size];
    let stat_key = format!("out-total-{}", cfg.service_name);
    let conn_key = format!("out-conn-{}", cfg.service_name);
    add_connection(&conn_key).await;
    let mut with_quit = String::new();
    let (mut in_bytes, mut out_bytes, mut error_count) = (0, 0, 0);

    loop {
        tokio::select! {
            read_result = reader.read(&mut read_buffer) => {
                match read_result {
                    Ok(0) => {
                        with_quit = format!("Connection {} closed by peer for {}", serv, part_uuid(&transfer));
                        break;
                    },
                    Ok(n) => {
                        // Reuse the persistent buffer; Bytes copies only n bytes.
                        let data = Bytes::copy_from_slice(&read_buffer[..n]);
                        match to_client_channel.send((transfer, cfg.service_code, data)).await {
                            Ok(_) => {
                                in_bytes += n;
                            },
                            Err(err) => {
                                error_count += 1;
                                warn!("Failed to forward data to client {} in {}: {}", part_uuid(&transfer), serv, err);
                                break;
                            }
                        }
                    },
                    Err(err) => {
                        with_quit = format!("Connection {} closed due to read error: {}", serv, err);
                        error_count += 1;
                        break;
                    }
                }
            },
            Some((_, data)) = from_client_channel.recv() => {
                if data.is_empty() {
                    info!("Connection {} closed by request for {}", serv, part_uuid(&transfer));
                    with_quit.clear();
                    break;
                }
                if let Err(err) = writer.write_all(data.as_ref()).await {
                    error_count += 1;
                    warn!("Failed to write to TCP stream for {} in {}: {}", part_uuid(&transfer), serv, err);
                    with_quit = "Connection closed due to target write error".to_string();
                    break;
                } else {
                    out_bytes += data.len();
                }
            },
            _ = sleep(cfg.idle_limit) => {
                info!("Idle timeout for {}", serv);
                if in_bytes + out_bytes + error_count > 0 {
                    update_traffic_stats(&stat_key, in_bytes, out_bytes, error_count).await;
                    (in_bytes, out_bytes, error_count) = (0, 0, 0);
                }
                break;
            }
        }
    }

    // Drain remaining inbound data before closing the target connection.
    loop {
        tokio::select! {
            Some((_, data)) = from_client_channel.recv() => {
                if !data.is_empty() {
                    match writer.write_all(data.as_ref()).await {
                        Ok(_) => {},
                        Err(err) => {
                            warn!("Failed to write remaining data to TCP stream for {} in {}: {}", part_uuid(&transfer), serv, err);
                            break;
                        }
                    }
                } else {
                    with_quit.clear();
                }
            },
            _ = sleep(cfg.collect_timeout) => break,
        }
    }

    if !with_quit.is_empty() {
        info!("{}", with_quit);
        let empty = Bytes::new();
        match to_client_channel.send((transfer, cfg.service_code, empty)).await {
            Ok(_) => {},
            Err(err) => warn!("Failed to send quit message for {}: {}", serv, err),
        }
    }
    if in_bytes + out_bytes + error_count > 0 {
        update_traffic_stats(&stat_key, in_bytes, out_bytes, error_count).await;
    }
    lost_connection(&conn_key).await;
}

async fn process_data_transfer_udp(
    cfg: TargetConfig,
    transfer: Uuid,
    mut from_client_channel: mpsc::Receiver<(Uuid, Bytes)>,
    to_client_channel: mpsc::Sender<(Uuid, Uuid, Bytes)>,
) {
    let serv = format!("{} ({}) - {}:{}", cfg.service_name, part_uuid(&cfg.service_code), cfg.ip, cfg.port);
    let stat_key = format!("out-total-{}", cfg.service_name);
    let target_addr: SocketAddr = (cfg.ip, cfg.port).into();
    // Bind a dedicated socket for this transfer.
    let bind_from = SocketAddr::new(cfg.udp_bind_ip, create_outbound_port(transfer));
    let socket = match UdpSocket::bind(&bind_from).await {
        Ok(socket) => {
            info!("New output socket {} -> {} for {}", bind_from, target_addr, part_uuid(&transfer));
            socket
        },
        Err(err) => {
            error!("Error binding UDP socket {} for {} in {}: {}", bind_from, part_uuid(&transfer), serv, err);
            let empty = Bytes::new();
            let _ = to_client_channel.send((transfer, cfg.service_code, empty)).await;
            return;
        }
    };

    let conn_key = format!("out-conn-{}", cfg.service_name);
    add_connection(&conn_key).await;

    let mut read_buffer = vec![0u8; cfg.buffer_size];
    let (mut in_bytes, mut out_bytes, mut error_count) = (0, 0, 0);
    loop {
        tokio::select! {
            read_result = socket.recv_from(&mut read_buffer) => {
                match read_result {
                    Ok((n, _addr)) => {
                        if n == 0 {
                            continue;
                        }
                        let data = Bytes::copy_from_slice(&read_buffer[..n]);
                        match to_client_channel.send((transfer, cfg.service_code, data)).await {
                            Ok(_) => { in_bytes += n; },
                            Err(err) => {
                                error_count += 1;
                                warn!("Failed to forward UDP response for {} in {}: {}", part_uuid(&transfer), serv, err);
                                break;
                            }
                        }
                    },
                    Err(err) => {
                        error_count += 1;
                        warn!("Read error on UDP socket for {} in {}: {}", part_uuid(&transfer), serv, err);
                        break;
                    }
                }
            },
            Some((_, data)) = from_client_channel.recv() => {
                if data.is_empty() {
                    info!("Connection {} closed by request for {}", serv, part_uuid(&transfer));
                    let empty = Bytes::new();
                    match to_client_channel.send((transfer, cfg.service_code, empty)).await {
                        Ok(_) => sleep(cfg.collect_timeout).await,
                        Err(err) => warn!("Failed to send quit message for {}: {}", serv, err),
                    }
                    break;
                }
                match socket.send_to(data.as_ref(), target_addr).await {
                    Ok(n) => { out_bytes += n; },
                    Err(err) => {
                        error_count += 1;
                        warn!("Failed to send UDP data for {} in {}: {}", part_uuid(&transfer), serv, err);
                        break;
                    }
                }
            },
            _ = sleep(cfg.idle_limit) => {
                if in_bytes + out_bytes + error_count > 0 {
                    update_traffic_stats(&stat_key, in_bytes, out_bytes, error_count).await;
                    (in_bytes, out_bytes, error_count) = (0, 0, 0);
                }
            }
        }
    }

    drop(socket);
    if in_bytes + out_bytes + error_count > 0 {
        update_traffic_stats(&stat_key, in_bytes, out_bytes, error_count).await;
    }
    lost_connection(&conn_key).await;
}

async fn handle_connection(
    stream: TcpStream,
    peer_addr: std::net::SocketAddr,
    data_handler: Arc<DataHandlerSettings>,
    settings: &Arc<Settings>,
) {
    let ip_str = peer_addr.ip().to_string();
    // Drop the connection if allowed networks are configured and the client is outside them.
    if !settings.in_subnet(&ip_str) {
        warn!("Unauthorized connection from {} (not in allowed networks), closing", ip_str);
        return;
    }

    let mut stat_key = format!("unknown-{}", ip_str);
    let idle_limit = settings.idle_tcp_limit;
    let stat_save_iter = settings.stat_save_iter;

    add_connection(&ip_str).await;
    info!("Server connection from {} ({})", peer_addr, ip_str);
    let (server_channel_size, service_out_channel_size) = settings.channel_size();
    set_channel_size(service_out_channel_size).await;
    let mut framed = Framed::new(stream, LengthDelimitedCodec::new());
    let (serv_tx, mut serv_rx) = mpsc::channel(server_channel_size);
    let (mut in_bytes, mut out_bytes, mut error_count) = (0, 0, 0);
    // Transfer ids observed on this connection (used for post-loop quit notification).
    let mut seen_transfers: HashSet<Uuid> = HashSet::new();
    // True after the first outbound framed.send() failure: further socket sends are skipped.
    let mut send_dead = false;
    // Number of valid MQTT frames with undecryptable content or unknown service for this peer.
    let mut wrong_attempt = 0;

    loop {
        tokio::select! {
            frame_opt = framed.next() => {
                match frame_opt {
                    Some(Ok(frame)) => {
                        let bytes_slice: &[u8] = frame.as_ref();
                        let iter_in_bytes = bytes_slice.len();
                        if iter_in_bytes < 1 {
                            continue
                        }
                        in_bytes += iter_in_bytes;
                        match data_handler.load_data_message(bytes_slice) {
                            Ok((msg, transfer_id)) => {
                                match resolve_target(&msg.service, settings) {
                                    Some(cfg) => {
                                        if !(exists(&transfer_id).await) {
                                            info!(
                                                "New transfering from {} service={}({}) target={}:{} proto={} transfer={}",
                                                peer_addr,
                                                cfg.service_name,
                                                part_uuid(&cfg.service_code),
                                                cfg.ip,
                                                cfg.port,
                                                if cfg.is_udp {"udp"} else {"tcp"},
                                                part_uuid(&transfer_id)
                                            );
                                            stat_key = format!("{}-{}", ip_str, cfg.service_name);
                                            add_connection(&ip_str).await;
                                            let client_in_channel = add_route(&transfer_id).await;
                                            let client_out_channel = serv_tx.clone();
                                            tokio::spawn(async move {
                                                if cfg.is_udp {
                                                    process_data_transfer_udp(cfg, transfer_id, client_in_channel, client_out_channel).await;
                                                } else {
                                                    process_data_transfer_tcp(cfg, transfer_id, client_in_channel, client_out_channel).await;
                                                }
                                            });
                                        }
                                        if !(send_data(&transfer_id, &msg.data).await) {
                                            error_count += 1;
                                            // Failed delivery: remove the transfer from routing so no further logic runs for it.
                                            if exists(&transfer_id).await {
                                                warn!("Failed send_data, removing route {}", part_uuid(&transfer_id));
                                                remove_route(&transfer_id).await;
                                            }
                                        }
                                    },
                                    None => {
                                        error_count += 1;
                                        wrong_attempt += 1;
                                        update_metric(&format!("suspicious-{}", ip_str), wrong_attempt).await;
                                        warn!(
                                            "Service key {} not configured on server (from {}, transfer: {})",
                                            part_uuid(&msg.service), peer_addr, part_uuid(&transfer_id)
                                        );
                                        break;
                                    }
                                }
                            },
                            Err(DataMessageError::BadPayload) => {
                                // Valid MQTT structure but content cannot be decrypted.
                                error_count += 1;
                                wrong_attempt += 1;
                                update_metric(&format!("suspicious-{}", ip_str), wrong_attempt).await;
                                warn!("Suspicious message from {} (valid structure, undecryptable content)", peer_addr);
                                break;
                            },
                            Err(err) => {
                                error_count += 1;
                                error!("Invalid message format from {}: {}", peer_addr, err);
                                break;
                            }
                        }
                    },
                    Some(Err(err)) => {
                        error_count += 1;
                        error!("Frame read error from {}: {}", peer_addr, err);
                        break;
                    },
                    None => {
                        info!("Client disconnected: {}", peer_addr);
                        break;
                    }
                }
            },
            Some((out_transfer, service, out_data)) = serv_rx.recv() => {
                // Once send_dead, skip socket writes (peer unreachable) but keep consuming queue for the drain.
                if !send_dead {
                    let packet = if out_data.is_empty() {
                        data_handler.make_quit_message(&service, &out_transfer)
                    } else {
                        data_handler.make_data_message(out_data.as_ref(), &service, &out_transfer)
                    };
                    out_bytes += packet.len();
                    match framed.send(packet).await {
                        Ok(_) => {
                            debug!(
                                "Sent to {} transfer={} bytes={}", peer_addr, part_uuid(&out_transfer), out_data.len()
                            );
                        },
                        Err(err) => {
                            error_count += 1;
                            send_dead = true;
                            warn!(
                                "Send error for {} (transfer={}, bytes={}): {}", peer_addr, part_uuid(&out_transfer), out_data.len(), err
                            );
                            seen_transfers.insert(out_transfer);
                        }
                    }
                } else {
                    info!("Send skipped (connection dead) transfer={}", part_uuid(&out_transfer));
                    seen_transfers.insert(out_transfer);
                }
            },
            _ = tokio::time::sleep(idle_limit) => {
                warn!("Idle timeout for connection from {} ({})", peer_addr, stat_key);
                break;
            },
            // Periodic traffic statistics update (mirrors client processing loops).
            _ = sleep(stat_save_iter) => {
                if in_bytes + out_bytes + error_count > 0 {
                    update_traffic_stats(&stat_key, in_bytes, out_bytes, error_count).await;
                    (in_bytes, out_bytes, error_count) = (0, 0, 0);
                }
            }
        }
    }
    // Drain remaining queue after send failure, bounded by drain_total() so it always terminates.
    if send_dead {
        let deadline = tokio::time::Instant::now() + settings.drain_total();
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() { break; }
            let recv_result = tokio::time::timeout(remaining, serv_rx.recv()).await;
            match recv_result {
                Ok(Some((drain_transfer, _, _))) => { seen_transfers.insert(drain_transfer); }
                _ => break,
            }
        }
    }
    // Broadcast quit (empty data) to every transfer seen on this connection so peers close cleanly.
    if send_dead && !seen_transfers.is_empty() {
        warn!("Broadcasting quit to {} transfers for {}", seen_transfers.len(), peer_addr);
        let total_deadline = tokio::time::Instant::now() + settings.drain_total();
        for transfer in &seen_transfers {
            let t = *transfer;
            let remaining = total_deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() { break; }
            // Bounded by per-send timeout: a non-draining receiver cannot block the loop.
            let _ = tokio::time::timeout(remaining, async move {
                let empty = Bytes::new();
                send_data(&t, &empty).await;
            }).await;
        }
    }

    if in_bytes + out_bytes + error_count > 0 {
        update_traffic_stats(&stat_key, in_bytes, out_bytes, error_count).await;
    }
    lost_connection(&ip_str).await;
    info!("Connection closed from {} ({})", ip_str, stat_key);
}

pub async fn run(settings: Settings) {
    if settings.tcp_targets.is_empty() && settings.udp_targets.is_empty() {
        error!("No TCP or UDP targets configured for server mode");
        std::process::exit(1);
    }

    let addr = format!("{}:{}", settings.server_host, settings.server_port);
    info!(
        "<<<<Starting>>>> Mode: server | Loading Level: {} | Buffer Size: {}",
        settings.loading_level, settings.buffer_size
    );
    info!("Server binding to: {}", addr);
    SERVER_RUNNING.store(true, Ordering::SeqCst);
    let (_service_in_channel_size, service_out_channel_size) = settings.channel_size();
    set_channel_size(service_out_channel_size).await;

    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            error!("Failed to bind server to {}: {}", addr, e);
            SERVER_RUNNING.store(false, Ordering::SeqCst);
            return;
        }
    };

    let cleanup_interval = settings.route_cleanup_interval();
    tokio::spawn(async move {
        run_cleanup(cleanup_interval).await;
    });

    info!("Transfer server listening on {}", addr);
    let data_handler = Arc::new(DataHandlerSettings::new(&settings));
    let settings_arc = Arc::new(settings.clone());
    let check_delay = settings.service_delay();

    let mut tasks: JoinSet<TaskResultEnum> = JoinSet::new();
    tasks.spawn(async move {
        loop {
            if let Ok((stream, peer_addr)) = listener.accept().await {
                let dh = data_handler.clone();
                let sa = settings_arc.clone();
                tokio::spawn(async move {
                    handle_connection(stream, peer_addr, dh, &sa).await;
                });
            } else {
                break;
            }
        }
        TaskResultEnum::WorkerDone
    });

    // Stop watcher: returns StopService when stop() is called.
    tasks.spawn(async move {
        loop {
            sleep(check_delay).await;
            if !is_running() {
                break;
            }
        }
        TaskResultEnum::StopService
    });

    // Periodic statistics dump to file with memory/uptime metrics.
    let stat_delay = settings.stat_delay;
    let stat_filepath = settings.stat_filepath.clone();
    tasks.spawn(async move {
        periodic_dump(&stat_filepath, stat_delay).await;
        TaskResultEnum::WorkerDone
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

    SERVER_RUNNING.store(false, Ordering::SeqCst);
    info!("Terminated tasks: {}. Server stopped", count_err);
}

#[cfg(test)]
mod tests {
    use super::*;
    use once_cell::sync::Lazy;
    use std::collections::HashMap;
    use tokio::sync::Mutex;

    static TEST_MUTEX: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

    async fn test_guard() -> tokio::sync::MutexGuard<'static, ()> {
        TEST_MUTEX.lock().await
    }

    fn base_settings() -> Settings {
        Settings {
            is_server: true,
            server_host: String::new(),
            server_port: 0,
            workers: 0,
            buffer_size: 4096,
            stat_delay: Duration::from_secs(180),
            stat_save_iter: Duration::from_secs(2),
            stat_filepath: "/tmp/stat.txt".to_string(),
            tcp_sockets: HashMap::new(),
            udp_sockets: HashMap::new(),
            tcp_targets: HashMap::new(),
            udp_targets: HashMap::new(),
            cipher_key: String::new(),
            idle_tcp_limit: Duration::from_secs(180),
            idle_udp_limit: Duration::from_secs(120),
            udp_bind_from: "0.0.0.0:0".to_string(),
            loading_level: crate::settings::LoadingLevelEnum::Default,
            networks: Vec::new(),
            client_name: Uuid::new_v4(),
        }
    }

    fn add_target<I: Into<IpAddr>>(map: &mut HashMap<String, HashMap<IpAddr, u16>>, name: &str, ip: I, port: u16) {
        map.entry(name.to_string()).or_insert_with(HashMap::new).insert(ip.into(), port);
    }

    #[tokio::test]
    async fn test_server_is_running_and_stop() {
        let _g = test_guard().await;
        assert!(!is_running(), "fresh process flag must start as not running");
        SERVER_RUNNING.store(true, Ordering::SeqCst);
        assert!(is_running());
        stop();
        assert!(!is_running());
    }

    #[test]
    fn test_create_outbound_port_deterministic_and_in_range() {
        let u = Uuid::new_v4();
        let first = create_outbound_port(u);
        for _ in 0..32 {
            assert_eq!(create_outbound_port(u), first, "same transfer id must map to the same port");
        }
        // remainder is in [0, u16::MAX-257]; +240 then wrapping shift of at most 31 stays high or wraps below 15
        for _ in 0..64 {
            let p = create_outbound_port(Uuid::new_v4());
            assert!(p >= 240 || p <= 14, "port {} out of expected range", p);
        }
    }

    #[test]
    fn test_udp_bind_ip() {
        let mut s = base_settings();
        s.udp_bind_from = "192.168.1.7:4000".to_string();
        assert_eq!(udp_bind_ip(&s), IpAddr::V4(Ipv4Addr::new(192, 168, 1, 7)));
        s.udp_bind_from = "[fe80::1]:9999".to_string();
        assert_eq!(udp_bind_ip(&s), "fe80::1".parse::<IpAddr>().unwrap());
        s.udp_bind_from = "not-an-address".to_string();
        assert_eq!(udp_bind_ip(&s), IpAddr::V4(Ipv4Addr::UNSPECIFIED), "invalid bind string falls back to any");
    }

    #[tokio::test]
    async fn test_resolve_target_missing_or_empty() {
        let s = base_settings();
        assert!(resolve_target(&Uuid::new_v4(), &s).is_none());
        let mut empty_map: HashMap<String, HashMap<IpAddr, u16>> = HashMap::new();
        empty_map.insert("svc-empty".to_string(), HashMap::new());
        let mut s2 = base_settings();
        s2.tcp_targets = empty_map;
        assert!(resolve_target(&code_name("svc-empty"), &s2).is_none(), "empty inner map must be skipped");
        let mut s3 = base_settings();
        add_target(&mut s3.udp_targets, "other", Ipv4Addr::new(10, 0, 0, 5), 80);
        assert!(resolve_target(&code_name("svc-empty"), &s3).is_none(), "name not in any target list");
    }

    #[tokio::test]
    async fn test_resolve_target_tcp_and_udp() {
        let mut s = base_settings();
        add_target(&mut s.tcp_targets, "svc-tcp", Ipv4Addr::new(127, 0, 0, 1), 9000);
        add_target(&mut s.udp_targets, "svc-udp", Ipv4Addr::new(127, 0, 0, 2), 9001);

        let tcp = resolve_target(&code_name("svc-tcp"), &s).expect("tcp target must resolve");
        assert_eq!(tcp.service_code, code_name("svc-tcp"));
        assert_eq!(tcp.ip, Ipv4Addr::new(127, 0, 0, 1));
        assert_eq!(tcp.port, 9000);
        assert!(!tcp.is_udp);
        assert_eq!(tcp.idle_limit, Duration::from_secs(180));
        assert_eq!(tcp.collect_timeout, s.collect_message_timeout(true), "final drain timeout is used");
        assert_eq!(tcp.buffer_size, 4096);

        let udp = resolve_target(&code_name("svc-udp"), &s).expect("udp target must resolve");
        assert_eq!(udp.service_code, code_name("svc-udp"));
        assert_eq!(udp.ip, Ipv4Addr::new(127, 0, 0, 2));
        assert_eq!(udp.port, 9001);
        assert!(udp.is_udp);
        assert_eq!(udp.idle_limit, Duration::from_secs(120), "udp idle limit must differ from tcp");
    }

    #[tokio::test]
    async fn test_resolve_target_prefers_tcp_when_both_listed() {
        let mut s = base_settings();
        add_target(&mut s.tcp_targets, "dup-svc", Ipv4Addr::new(127, 0, 0, 1), 9000);
        add_target(&mut s.udp_targets, "dup-svc", Ipv4Addr::new(127, 0, 0, 2), 9001);
        let cfg = resolve_target(&code_name("dup-svc"), &s).expect("duplicated service must resolve");
        assert!(!cfg.is_udp, "tcp targets are looked up before udp ones");
        assert_eq!(cfg.port, 9000);
    }
}
