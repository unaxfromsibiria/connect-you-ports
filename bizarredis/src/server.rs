use log::{info, error, debug, warn};
use tokio::sync::mpsc;
use tokio::net::{TcpStream, TcpListener};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::sleep;
use once_cell::sync::Lazy;
use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::str::FromStr;
use std::time::Duration;
use tokio::net::UdpSocket;
use uuid::Uuid;
use crate::stat::{add_connection, lost_connection, update_traffic_stats, update_metric};
use crate::data::{DataHandler, DataHandlerSettings, DataMsg};
use crate::common::{OUT_TTL, Settings, LoadingParams, IpPortMap, code_name, CMD_BUF_SIZE, part_uuid};


static KNOWN_COMMANDS: Lazy<HashSet<&'static str>> = Lazy::new(|| {[
    "get", "set", "auth", "put", "setex", "keys", "del", "exists", "expire", "ttl", "type",
    "append", "incr", "decr", "mget", "mset", "lpush", "rpush", "lpop", "rpop", "lrange",
    "sadd", "srem", "smembers", "scard", "hset", "hget", "hdel", "hkeys", "hvals",
    "zadd", "zrem", "zrange", "zcard", "ping", "echo", "select", "dbsize", "flushdb",
    "quit", "shutdown", "config", "info", "subscribe", "publish", "unsubscribe",
    "eval", "evalsha", "geoadd", "georadius", "spop", "srandermember", "hincrby", "hincrbyfloat",
    "bitcount", "bitop", "sort", "debug", "monitor", "client", "cluster", "scan", "hscan", "sscan", "zscan",
    "move", "rename", "renamenx", "restore", "persist", "pttl", "dump", "object", "expireat", "pexpire", "pexpireat",
    "migrate", "dump", "restore", "wait", "blpop", "brpop", "brpoplpush", "linsert", "lset", "ltrim",
    "sinter", "sinterstore", "sunion", "sunionstore", "sdiff", "sdiffstore", "zinterstore", "zunionstore", "zincrby",
    "zrank", "zrevrank", "hstrlen", "hmget", "hexists", "hlen", "hrandfield", "pfadd", "pfcount", "pfmerge",
    "geodist", "geohash", "geopos", "georadiusbymember", "xadd", "xdel", "xlen", "xrange",
    "xrevrange", "xgroup", "xread", "ts", "json", "ft", "function",
].into_iter().collect()});


/// Handles data forwarding between a client and a TCP target service
async fn handle_target_tcp_transfering(
    transfer: Uuid,
    settings: Arc<Settings>,
    service_code: Uuid,
    service_name: String,
    mut in_data_channel: mpsc::Receiver<DataMsg>,
    out_channel: mpsc::Sender<DataMsg>,
    target_host: IpAddr,
    target_port: u16,
    data_handler: Arc<DataHandlerSettings>,
) {
    let tcp_stream = match TcpStream::connect((target_host, target_port)).await {
        Ok(stream) => {stream},
        Err(err) => {
            error!("Failed to connect to TCP target '{}' {}:{} : {}", service_name, target_host, target_port, err);
            return
        }
    };
    let buffer_size= settings.buffer_size;
    if tcp_stream.set_ttl(OUT_TTL).is_err() {
        warn!("TTL is {}", tcp_stream.ttl().unwrap());
    }
    tcp_stream.set_nodelay(true).unwrap();
    let (mut reader, mut writer) = tokio::io::split(tcp_stream);
    let mut read_buffer = vec![0u8; buffer_size];
    let serv = format!("{} ({}) - {}:{}", service_name, part_uuid(&service_code), target_host, target_port);
    let idle_limit = settings.idle_tcp_limit;
    let mut with_quit = "".to_string();
    let wait_before_close_time = settings.collect_message_timeout(true);
    let (mut in_bytes, mut out_bytes, mut error_count) = (0, 0, 0);
    let stat_key = format!("out-total-{}", service_name);

    loop {
        tokio::select! {
            read_result = reader.read(&mut read_buffer) => {
                match read_result {
                    Ok(0) => {
                        with_quit = format!("Connection {} closed by peer for {}", serv, part_uuid(&transfer));
                        break;
                    },
                    Ok(n) => {
                        let data = &read_buffer[..n];
                        let msg = data_handler.make_data_message(&data, &service_code, &transfer);
                        match out_channel.send(msg).await {
                            Ok(_) => {
                                in_bytes += n;
                            },
                            Err(err) => {
                                error_count += 1;
                                debug!("Failed to forward data to client {} in {}: {}", transfer, serv, err);
                                break;
                            }
                        }
                    },
                    Err(err) => {
                        debug!("Failed to read from TCP stream for {} {}: {}", transfer, serv, err);
                        with_quit = format!("Connection {} closed due to read error: {}", serv, part_uuid(&transfer));
                        error_count += 1;
                        break;
                    }
                }
            },
            Some(msg) = in_data_channel.recv() => {
                if msg.x {
                    info!("Connection {} closed by request for {}", serv, transfer);
                    with_quit = "".to_string();
                    break;
                }
                if let Err(err) = writer.write_all(&msg.d).await {
                    error_count += 1;
                    error!("Failed to write to TCP stream for {} {}: {}", transfer, serv, err);
                    with_quit = "Connection write error".to_string();
                    break;
                } else {
                    out_bytes += msg.d.len();
                }
            },
            _ = sleep(idle_limit) => {
                if in_bytes + out_bytes + error_count > 0 {
                    update_traffic_stats(&stat_key, in_bytes, out_bytes, error_count).await;
                    (in_bytes, out_bytes, error_count) = (0, 0, 0);
                }
                break;
            }
        }
    }
    // final data transfering operations
    loop {
        tokio::select! {
            Some(msg) = in_data_channel.recv() => {
                if msg.x {
                    continue;
                }
                if let Err(err) = writer.write_all(&msg.d).await {
                    error!("Failed to write remaining data to TCP stream for {} {}: {}", part_uuid(&transfer), serv, err);
                    break;
                }
            },
            _ = sleep(wait_before_close_time) => {
                if !with_quit.is_empty() {
                    info!("{} (sending quit request)", with_quit);
                    let msg = data_handler.make_quit_message(&service_code, &transfer);
                    match out_channel.send(msg).await {
                        Ok(_) => {},
                        Err(err) => {
                            warn!("Failed to send quit message for {} due to channel error: {}", serv, err);
                        }
                    }
                }
                break;
            }
        }
    }
    if in_bytes + out_bytes + error_count > 0 {
        update_traffic_stats(&stat_key, in_bytes, out_bytes, error_count).await;
    }
}

/// Handles data forwarding between a client and a UDP target service
async fn handle_target_udp_transfering(
    transfer: Uuid,
    settings: Arc<Settings>,
    service_code: Uuid,
    service_name: String,
    mut in_data_channel: mpsc::Receiver<DataMsg>,
    out_channel: mpsc::Sender<DataMsg>,
    target_host: IpAddr,
    target_port: u16,
    data_handler: Arc<DataHandlerSettings>,
) {
    let udp_bind_from = match SocketAddr::from_str(&settings.udp_bind_from) {
        Ok(addr_new) => addr_new,
        Err(err) => {
            error!("Incorrect address in settings {}: {}", settings.udp_bind_from, err);
            return;
        }
    };
    let socket= match UdpSocket::bind(&udp_bind_from).await {
        Ok(socket) => {
            info!("New output socket {} -> {}:{} for {}", udp_bind_from, target_host, target_port, transfer);
            socket
        },
        Err(err) => {
            error!("Error binding UDP socket {} for {} service {}: {}", udp_bind_from, transfer, service_code, err);
            return;
        }
    };
    let buffer_size = settings.buffer_size;
    let idle_limit = settings.idle_udp_limit;
    let mut read_buffer = vec![0u8; buffer_size];
    let serv = format!("{} ({}) - {}:{}", service_name, service_code, target_host, target_port);
    let mut with_quit;
    let (mut in_bytes, mut out_bytes, mut error_count) = (0, 0, 0);
    let target_addr = (target_host, target_port);
    let stat_key = format!("out-total-{}", service_name);
    let mut current_transfer = transfer.clone();

    loop {
        tokio::select! {
            read_result = socket.recv_from(&mut read_buffer) => {
                match read_result {
                    Ok((n, addr)) => {
                        debug!("Service {} response size: {}", service_name, n);
                        if n == 0 {
                            with_quit = format!("Connection {} closed by peer (0 bytes) from {} for {}", serv, addr, transfer);
                            break;
                        }
                        let data = &read_buffer[..n];
                        let msg = data_handler.make_data_message(&data, &service_code, &current_transfer);
                        match out_channel.send(msg).await {
                            Ok(_) => {
                                in_bytes += n;
                            },
                            Err(err) => {
                                error_count += 1;
                                error!("Failed to forward data to client {} in {}: {}", transfer, serv, err);
                                continue;
                            }
                        }
                    },
                    Err(err) => {
                        error!("Failed to read from UDP socket for {} {}: {}", transfer, serv, err);
                        with_quit = format!("Connection {} closed due to read error: {}", serv, transfer);
                        error_count += 1;
                        break;
                    }
                }
            },
            Some(msg) = in_data_channel.recv() => {
                if msg.x {
                    info!("Connection {} closed by request for {}", serv, transfer);
                    with_quit = "".to_string();
                    break;
                }
                current_transfer = msg.t;
                match socket.send_to(&msg.d, target_addr).await {
                    Ok(n) => {
                        debug!("Sent udp data size {} to {}", n, service_name);
                        out_bytes += n;
                    },
                    Err(err) => {
                        error_count += 1;
                        error!("Failed to send UDP data to {} ({}) from {}: {}", target_host, service_name, current_transfer, err);
                        with_quit = format!("Connection {} closed due to send error: {}", serv, transfer);
                        break;
                    }
                }
            },
            _ = sleep(idle_limit) => {
                if in_bytes + out_bytes + error_count > 0 {
                    update_traffic_stats(&stat_key, in_bytes, out_bytes, error_count).await;
                    (in_bytes, out_bytes, error_count) = (0, 0, 0);
                }
            }
        }
    }

    // Ensure quit message is logged and sent even if with_quit was not set by a specific break condition
    if with_quit.is_empty() {
        with_quit = format!("Connection {} closed unexpectedly", serv);
    } else {
        let msg = data_handler.make_quit_message(&service_code, &transfer);
        match out_channel.send(msg).await {
            Ok(_) => {
                sleep(settings.collect_message_timeout(false)).await;
            },
            Err(err) => {
                debug!("Failed to send quit message to client {} in {}: {}", transfer, serv, err);
            }
        }
    }
    info!("{}", with_quit);
    if in_bytes + out_bytes + error_count > 0 {
        update_traffic_stats(&stat_key, in_bytes, out_bytes, error_count).await;
    }
}

/// Starts the service connection loop, waiting for configuration or stop signals.
async fn start_service_connection(
    settings: Arc<Settings>,
    mut settings_channel: mpsc::Receiver<(Uuid, Uuid, String, IpAddr, u16, bool)>,
    in_data_channel: mpsc::Receiver<DataMsg>,
    out_channel: mpsc::Sender<DataMsg>,
    data_handler: Arc<DataHandlerSettings>
) {
    let transfer: Uuid;
    let s_code: Uuid;
    let s_name: String;
    let t_ip: IpAddr;
    let t_port: u16;
    let is_udp: bool;

    loop {
        if let Some(item) = settings_channel.recv().await {
            (transfer, s_code, s_name, t_ip, t_port, is_udp) = item;
            if transfer.is_nil() {
                info!("Connection service stopped: received nil transfer ID");
                return;
            } else {
                break;
            }
        } else {
            return;
        }
    }

    if is_udp {
        handle_target_udp_transfering(transfer, settings, s_code, s_name, in_data_channel, out_channel, t_ip, t_port, data_handler).await;
    } else {
        handle_target_tcp_transfering(transfer, settings, s_code, s_name, in_data_channel, out_channel, t_ip, t_port, data_handler).await;
    }
}

/// Extracts service code, target IP/port, and data size from the provided buffer.
fn extract_code(
    buf: &[u8],
    n: usize,
    tcp_targets: &IpPortMap,
    udp_targets: &IpPortMap,
) -> (Uuid, String, Option<std::net::IpAddr>, Option<u16>, bool, usize) {
    let mut cursor = 0;
    while cursor < n && buf[cursor].is_ascii_alphanumeric() {
        cursor += 1;
    }
    while cursor < n && buf[cursor].is_ascii_whitespace() {
        cursor += 1;
    }
    let code_start = cursor;
    while cursor < n && (buf[cursor].is_ascii_alphanumeric()) {
        cursor += 1;
    }
    let code_end = cursor;
    if code_start == code_end {
        return (Uuid::nil(), String::new(), None, None, false, 0);
    }
    let service_code_str = match std::str::from_utf8(&buf[code_start..code_end]) {
        Ok(s) => s.to_string(),
        Err(_) => {
            return (Uuid::nil(), String::new(), None, None, false, 0)
        },
    };
    let service_code = match Uuid::parse_str(&service_code_str) {
        Ok(val) => val,
        Err(_) => {
            warn!("Incorrect service code {}", service_code_str);
            return (Uuid::nil(), String::new(), None, None, false, 0)
        }
    };

    let expect_data_size: usize;
    if code_end + 5 <= buf.len() {
        let size_bytes = &buf[code_end + 1..code_end + 5];
        expect_data_size = u32::from_le_bytes(size_bytes.try_into().unwrap()) as usize;
    } else {
        // Not enough bytes to read a u32
        return (Uuid::nil(), String::new(), None, None, false, 0);
    }

    for (name, ip_map) in tcp_targets {
        if code_name(&name) == service_code {
            if let Some((ip, &port)) = ip_map.iter().next() {
                return (service_code, name.clone(), Some(ip.clone()), Some(port), false, expect_data_size);
            }
        }
    }

    for (name, ip_map) in udp_targets {
        if code_name(name.as_str()) == service_code {
            if let Some((ip, &port)) = ip_map.iter().next() {
                return (service_code, name.clone(), Some(ip.clone()), Some(port), true, expect_data_size);
            }
        }
    }

    (Uuid::nil(), String::new(), None, None, false, 0)
}

/// Starts the main server loop, accepting incoming TCP connections
/// and spawning tasks to handle client communication and data transfer.
pub async fn run_server(settings: Settings) {
    if settings.tcp_targets.is_empty() || settings.udp_targets.is_empty() {
        std::process::exit(1);
    }
    let addr = format!("{}:{}", settings.server_host, settings.server_port);
    let listener = match TcpListener::bind(&addr).await {
        Ok(tcp_l) => tcp_l,
        Err(e) => {
            error!("Failed to bind to {}: {}", addr, e);
            return;
        }
    };

    info!("Transfer server listening on {}", addr);
    let max_word_len = KNOWN_COMMANDS.iter().map(|c| c.len()).max().unwrap_or(10);
    let arc_data_handler = Arc::new(DataHandlerSettings::new(&settings));
    let arc_settings = Arc::new(settings.clone());

    loop {
        let (stream, peer_addr) = match listener.accept().await {
            Ok(s) => s,
            Err(e) => {
                error!("Connection failed from: {}", e);
                continue;
            }
        };
        info!("Connection from {}", peer_addr);
        if stream.set_ttl(OUT_TTL).is_err() {
            warn!("Current TTL is {}", stream.ttl().unwrap());
        }
        stream.set_nodelay(true).unwrap();
        let stat_save_iter = settings.stat_save_iter;
        let udp_targets = settings.udp_targets.clone();
        let tcp_targets = settings.tcp_targets.clone();
        let data_handler = arc_data_handler.clone();
        let (service_in_channel_size, service_out_channel_size) = settings.channel_size();
        let close_delay = settings.collect_message_timeout(true);
        let service_arc_settings = arc_settings.clone();
        let arc_settings = arc_settings.clone();
        let (server_out, mut server_in) = mpsc::channel(service_out_channel_size);
        let (client_out, client_in) = mpsc::channel(service_in_channel_size);
        let (settings_out, settings_in) = mpsc::channel(1);

        tokio::spawn(async move {
            start_service_connection(
                service_arc_settings, settings_in,client_in, server_out, data_handler
            ).await;
        });

        let data_handler = arc_data_handler.clone();
        tokio::spawn(async move {
            let mut cmd_buf = vec![0u8; CMD_BUF_SIZE];
            let mut to_close = false;
            let mut true_client = false;
            let mut latest_quit = true;
            let idle_tcp_limit = arc_settings.idle_tcp_limit;
            let idle_udp_limit = arc_settings.idle_udp_limit;
            let mut idle_limit = idle_tcp_limit;
            let (mut in_bytes, mut out_bytes, mut error_count) = (0, 0, 0);
            let mut cur_service = Uuid::nil();
            let mut cur_transfer_id = Uuid::nil();
            let ip_str = peer_addr.ip().to_string();
            let buffer_size_limit = arc_settings.buffer_size * 3;
            let mut stat_key = format!("unknown-{}", ip_str);
            let mut wrong_attempt = 0;
            let mut scan_attempt = 0;
            let mut response ;
            let (mut reader, mut writer) = tokio::io::split(stream);
            let mut get_size_problems = 0;
            add_connection(&ip_str).await;

            loop {
                tokio::select! {
                    res = reader.read(&mut cmd_buf[..]) => {
                        match res {
                            Ok(0) => {
                                break;
                            }
                            Ok(n) => {
                                response = if true_client {
                                     String::new()
                                } else {
                                    let part_len = (max_word_len + 1).min(n);
                                    let part = &cmd_buf[..part_len];
                                    let mut cmd_end_idx = part.len();
                                    
                                    for (i, &ch) in part.iter().enumerate() {
                                        if !ch.is_ascii_alphanumeric() && ch != b'_' {
                                            cmd_end_idx = i;
                                            break;
                                        }
                                    }
                                    let potential_cmd = &cmd_buf[..cmd_end_idx];
                                    match std::str::from_utf8(potential_cmd) {
                                        Ok(cmd_str) => {
                                            let lower_cmd = cmd_str.to_lowercase();
                                            if lower_cmd == "set" {
                                                String::new()
                                            } else if lower_cmd == "quit" {
                                                to_close = true;
                                                "+OK\r\n".to_string()
                                            } else if KNOWN_COMMANDS.contains(lower_cmd.as_str()) {
                                                scan_attempt += 1;
                                                update_metric(&format!("scan-{}", ip_str), scan_attempt).await;
                                                format!("-ERR wrong number of arguments for '{}' command\r\n", lower_cmd).to_string()
                                            } else {
                                                "-NOAUTH Authentication required.\r\n".to_string()
                                            }
                                        }
                                        Err(_) => {
                                            to_close = true;
                                            "-NOAUTH Authentication required.\r\n".to_string()
                                        }
                                    }
                                };
                                // server try to process
                                if response.is_empty() {
                                    let (
                                        code, service_name, in_target_ip, in_target_port, is_udp, expect_data_size,
                                    ) = extract_code(&cmd_buf, n, &tcp_targets, &udp_targets);
                                    debug!("Service code: {}, expected data size: {}", code, expect_data_size);
                                    if !arc_settings.in_subnet(&ip_str) {
                                        response = "-NOAUTH Authentication required.\r\n".to_string();
                                        if !code.is_nil() {
                                            warn!("Unauthorized API access attempt from forbidden network {}", ip_str);
                                        }
                                    } else if code.is_nil() {
                                        response = "-NOAUTH Authentication required.\r\n".to_string();
                                        to_close = true;
                                        wrong_attempt += 1;
                                        update_metric(&format!("suspicious-{}", ip_str), wrong_attempt).await;
                                    } else if let (Some(t_ip), Some(t_port)) = (in_target_ip, in_target_port) {
                                        if expect_data_size > buffer_size_limit {
                                            get_size_problems += 1;
                                            debug!(
                                                "Unexpectedly large data buffer size: {} bytes. Protocol limit is {} bytes.",
                                                expect_data_size,
                                                buffer_size_limit
                                            );
                                            continue;
                                        }
                                        stat_key = format!("{}-{}", ip_str, service_name);
                                        let mut data_slice = vec![0u8; expect_data_size];
                                        match reader.read_exact(&mut data_slice).await {
                                            Ok(_) => {
                                                to_close = false;
                                            },
                                            Err(err) => {
                                                to_close = true;
                                                error_count += 1;
                                                error!("Failed to read buffer {} in {}: {}", ip_str, service_name, err);
                                                continue;
                                            },   
                                        }
                                        match data_handler.load_data_message(&data_slice) {
                                            Ok(msg) => {
                                                if msg.x {
                                                    latest_quit = true;
                                                    info!("Connection {} is closing by request for {} ({})", ip_str, cur_transfer_id, service_name);
                                                    break;
                                                }
                                                if !true_client {
                                                    true_client = true;
                                                    cur_service = code.clone();
                                                    if is_udp {
                                                        idle_limit = idle_udp_limit;
                                                    }
                                                    let settings_item = (msg.t.clone(), code, service_name.clone(), t_ip, t_port, is_udp);
                                                    match settings_out.send(settings_item).await {
                                                        Ok(_) => info!("Starting service connection {} ({})", service_name, cur_service),
                                                        Err(err) => {
                                                            error!("Failed to send settings to client {} in {}: {}", ip_str, cur_service, err);
                                                            break;
                                                        }
                                                    }
                                                }
                                                if cur_transfer_id.is_nil() {
                                                    cur_transfer_id = msg.t.clone();
                                                }
                                                let m_size = msg.d.len();
                                                in_bytes += expect_data_size;
                                                match client_out.send(msg).await {
                                                    Ok(_) => {
                                                        debug!("Received client message with data size: {}", m_size);
                                                    },
                                                    Err(err) => {
                                                        error_count += 1;
                                                        to_close = true;
                                                        error!("Failed to send data to client {} in {}: {}", ip_str, service_name, err);
                                                        continue;
                                                    }
                                                }
                                            },
                                            Err(err) => {
                                                debug!("Incorrect data from {}", ip_str);
                                                to_close = true;
                                                if true_client {
                                                    warn!("Client sent invalid message: {}, from IP: {}", err, ip_str);
                                                }
                                            }
                                        }
                                        debug!("Message to {} ({}) status: {}", code, service_name, if true_client {"client"} else {"unknown"});
                                    }
                                }
                                if let Err(e) = writer.write_all(response.as_bytes()).await {
                                    error!("Write failed: {}", e);
                                    break;
                                } else {
                                    out_bytes += response.len();
                                    debug!("response: {}", response);
                                }
                            }
                            Err(e) => {
                                error!("Read error: {}", e);
                                break;
                            }
                        }
                        if to_close {
                            break;
                        }
                    },
                    Some(msg) = server_in.recv() => {
                        // write to connection
                        latest_quit = msg.x;
                        let (s_part, d_part) = msg.dump(false);
                        if let Err(err) = writer.write_all(&s_part).await {
                            error!("Failed to write to TCP stream {} {}: {}", ip_str, cur_service, err);
                            error_count += 1;
                        } else if let Err(err) = writer.write_all(&d_part).await {
                            error!("Failed to write to TCP stream {} {}: {}", ip_str, cur_service, err);
                            error_count += 1;
                        } else {
                            debug!("Client data {} has been sent for {} {}", s_part.len() + d_part.len(), cur_service, ip_str);
                            out_bytes += d_part.len() + s_part.len();
                        }
                    },
                    _ = sleep(idle_limit) => {
                        info!("Closing connection due to idle timeout for {} ({})", cur_service, cur_transfer_id);
                        latest_quit = false;
                        break;
                    },
                    _ = sleep(stat_save_iter) => {
                        if get_size_problems > 0 {
                            update_metric(&format!("{}-{}-wrong-size-block", cur_service, ip_str), get_size_problems).await;
                        }
                        if in_bytes + out_bytes + error_count > 0 {
                            update_traffic_stats(&stat_key, in_bytes, out_bytes, error_count).await;
                            (in_bytes, out_bytes, error_count) = (0, 0, 0);
                        }
                    }
                }
            }
            let and_wait = if !true_client {
                let settings_item = (
                    Uuid::nil(), Uuid::nil(), String::new(), IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0, false,
                );
                let _ = settings_out.send(settings_item).await;
                close_delay
            } else if !latest_quit {
                let quit_msg = data_handler.make_quit_message(&cur_service, &cur_transfer_id);
                match client_out.send(quit_msg).await {
                    Ok(_) => close_delay,
                    Err(err) => {
                        error!("Failed to send QUIT to client {} in {}: {}", peer_addr.ip(), cur_service, err);
                        Duration::from_secs(0)
                    }
                }
            } else {
                Duration::from_secs(0)
            };
            if in_bytes + out_bytes + error_count > 0 {
                update_traffic_stats(&stat_key, in_bytes, out_bytes, error_count).await;
            }
            lost_connection(&ip_str).await;
            info!("Connection closed from {}", peer_addr);
            sleep(and_wait).await;
        });
    }
}
