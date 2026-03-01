use std::env;
use std::collections::HashMap;
use std::net::IpAddr;
use uuid::Uuid;
use sha1::{Sha1, Digest};
use log::{info, warn};

// env variables
const ENV_IS_SERVER: &str = "SERVER";
const ENV_WORKERS: &str = "WORKERS";
const ENV_CLIENT_NAME: &str = "CLIENT_NAME";
const ENV_BUFFER_SIZE: &str = "READ_BUFFER_SIZE";
const ENV_TCP_SOCKETS: &str = "TCP_SOCKETS";
const ENV_UDP_SOCKETS: &str = "UDP_SOCKETS";
const ENV_STAT_SHOW_INTERVAL: &str = "STAT_SHOW_INTERVAL";
const ENV_TCP_TARGET: &str = "SERVER_TCP_TARGET";
const ENV_UDP_TARGET: &str = "SERVER_UDP_TARGET";
const ENV_REDIS: &str = "REDIS_URL";
const ENV_NEW_CONNECTION_DELAY: &str = "NEW_CONNECTION_DELAY";
const ENV_COMPRESSION: &str = "COMPRESS";
const ENV_KEY_CIPHER: &str = "CRYPTO_KEY";
const ENV_CONNECTION_IDLE: &str = "CONNECTION_IDLE_LIMIT";
const ENV_UDP_CONNECTION_IDLE: &str = "UDP_CONNECTION_IDLE_LIMIT";
const ENV_UDP_BIND_FROM: &str = "UDP_BIND_FROM";
pub const TOPIC_NAME_NEW_CLIENT: &str = "new-c";
pub const TOPIC_NAME_DATA_CLIENT: &str = "data-c";
pub const TOPIC_NAME_DATA_SERVER: &str = "data-s";
type IpPortMap = HashMap<String, HashMap<IpAddr, u16>>;

fn _read_env_socket_maps(name: &str, silent: bool) -> IpPortMap {
    let mut result = HashMap::new();
    let map_str = match env::var(name) {
        Ok(val) => val,
        Err(e) => {
            if !silent {
                warn!("Error getting {} as socket map: {}", name, e);
            }
            return result
        },
    };

    for entry in map_str.split(';') {
        let parts: Vec<&str> = entry.split(':').collect();
        if parts.len() != 3 {
            if !silent {
                warn!("Invalid socket entry format: {}", entry);
            }
            continue;
        }

        let service_name = parts[0].to_string();
        let ip_str = parts[1];
        let port_str = parts[2];
        let ip = match ip_str.parse::<IpAddr>() {
            Ok(ip) => ip,
            Err(e) => {
                if !silent {
                    warn!("Invalid IP address {}: {}", ip_str, e);
                }
                continue;
            }
        };

        let port = match port_str.parse::<u16>() {
            Ok(port) => port,
            Err(e) => {
                if !silent {
                    warn!("Invalid port {}: {}", port_str, e);
                }
                continue;
            }
        };
        result.entry(service_name).or_insert_with(HashMap::new).insert(ip, port);
    }
    result
}

fn _read_env_bool(name: &str, silent: bool, default: bool) -> bool {
    let bool_str = match env::var(name) {
        Ok(val) => val,
        Err(e) => {
            if !silent {
                warn!("Error getting {} as bool: {}", name, e);
            }
            "".to_string()
        },
    };
    let normalized = bool_str.to_lowercase();
    let key = normalized.trim();
    if normalized.is_empty() {
        return default;
    }
    let true_values = ["on", "yes", "1", "true", "ok"];
    true_values.contains(&key)
}

fn _read_env_str(name: &str, silent: bool) -> String {
    match env::var(name) {
        Ok(val) => val,
        Err(e) => {
            if !silent {
                warn!("Error getting {} as string: {}", name, e);
            }
            "".to_string()
        },
    }
}

fn _read_env_uint(name: &str, silent: bool, default: usize) -> usize {
    match env::var(name) {
        Ok(val) => match val.parse::<usize>() {
            Ok(num) => num,
            Err(e) => {
                if !silent {
                    warn!("Error parsing {} as uint: {}", name, e);
                }
                default
            },
        },
        Err(e) => {
            if !silent {
                warn!("Error getting {} as uint: {}", name, e);
            }
            default
        },
    }
}

fn _read_env_strings(name: &str, silent: bool) -> Vec<String> {
    match env::var(name) {
        Ok(val) => {
            val.split(';').map(|s| s.trim().to_string()).collect()
        },
        Err(e) => {
            if !silent {
                warn!("Error getting {} as strings: {}", name, e);
            }
            Vec::new()
        },
    }
}

#[derive(Clone)]
pub struct Settings {
    pub is_server: bool,
    pub workers: usize,
    pub redis_conn: String,
    pub buffer_size: usize,
    pub stat_delay: usize,
    pub client_name: String,
    pub tcp_sockets: IpPortMap,
    pub udp_sockets: IpPortMap,
    pub tcp_targets: IpPortMap,
    pub udp_targets: IpPortMap,
    pub new_connection_delay: usize,
    pub cipher_key: String,
    pub use_compression: bool,
    pub idle_tcp_limit: usize,
    pub idle_udp_limit: usize,
    pub udp_bind_from: String,
}

pub fn fast_name() -> String {
    let val = Uuid::new_v4().to_string();
    let parts: Vec<&str> = val.split('-').collect();
    format!("{}{}", parts[0], &parts[4][..4])
}

pub fn code_name(value: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(value);
    let result = hasher.finalize();
    result[..10].iter().map(|b| format!("{:02x}", b)).collect()
}

pub fn create_settings() -> Settings {
    let is_server = _read_env_bool(ENV_IS_SERVER, true, false);
    let use_compression = _read_env_bool(ENV_COMPRESSION, true, false);
    let cipher_key = _read_env_str(ENV_KEY_CIPHER, true);
    let buffer_size = _read_env_uint(ENV_BUFFER_SIZE, true, 1024 * 8);
    let workers = _read_env_uint(ENV_WORKERS, true, 4);
    let new_connection_delay = _read_env_uint(ENV_NEW_CONNECTION_DELAY, true, 200);
    let mut client_name = _read_env_str(ENV_CLIENT_NAME, true);
    if client_name.is_empty() {
        client_name = fast_name();
    }
    let stat_delay = _read_env_uint(ENV_STAT_SHOW_INTERVAL, true, 120);
    let tcp_sockets = if !is_server {
        _read_env_socket_maps(ENV_TCP_SOCKETS, false)
    } else {
        IpPortMap::new()
    };
    let udp_sockets = if !is_server {
        _read_env_socket_maps(ENV_UDP_SOCKETS, false)
    } else {
        IpPortMap::new()
    };
    let tcp_targets = if is_server {
        _read_env_socket_maps(ENV_TCP_TARGET, false)
    } else {
        IpPortMap::new()
    };
    let udp_targets = if is_server {
        _read_env_socket_maps(ENV_UDP_TARGET, false)
    } else {
        IpPortMap::new()
    };
    let mut redis_conn = _read_env_str(ENV_REDIS, false);
    if redis_conn.is_empty() {
        redis_conn = "redis://127.0.0.1:6379/".to_string();
        info!("Default Redis connection is being used.");
    }
    let idle_tcp_limit = _read_env_uint(ENV_CONNECTION_IDLE, true, 60 * 3);
    let idle_udp_limit = _read_env_uint(ENV_UDP_CONNECTION_IDLE, true, 60);
    let mut udp_bind_from = _read_env_str(ENV_UDP_BIND_FROM, true);
    if udp_bind_from.is_empty() {
        udp_bind_from = "0.0.0.0:0".to_string();
    }

    Settings {
        is_server,
        workers,
        redis_conn,
        buffer_size,
        stat_delay,
        client_name,
        tcp_sockets,
        udp_sockets,
        tcp_targets,
        udp_targets,
        new_connection_delay,
        cipher_key,
        use_compression,
        idle_tcp_limit,
        idle_udp_limit,
        udp_bind_from,
    }
}
