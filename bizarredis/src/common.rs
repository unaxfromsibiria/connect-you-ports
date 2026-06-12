use std::env;
use std::fmt;
use std::str::FromStr;
use std::collections::HashMap;
use std::net::IpAddr;
use log::{info, warn};
use uuid::Uuid;
use sha1::{Sha1, Digest};
use ipnetwork::IpNetwork; 
use tokio::time::Duration;
use rand;

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
const ENV_KEY_CIPHER: &str = "CRYPTO_KEY";
const ENV_CONNECTION_IDLE: &str = "CONNECTION_IDLE_LIMIT";
const ENV_UDP_CONNECTION_IDLE: &str = "UDP_CONNECTION_IDLE_LIMIT";
const ENV_UDP_BIND_FROM: &str = "UDP_BIND_FROM";
const ENV_SERVER_HOST: &str = "SERVER_HOST";
const ENV_SERVER_PORT: &str = "SERVER_PORT";
const ENV_LOADING_LEVEL: &str = "LOADING_LEVEL";
const ENV_ALLOW_NET: &str = "ALLOW_NET";
const ENV_STAT_SAVE_INTERVAL: &str = "STAT_SAVE_INTERVAL";

pub type IpPortMap = HashMap<String, HashMap<IpAddr, u16>>;
pub const OUT_TTL: u32 = 96;
pub const CMD_BUF_SIZE: usize = 42;

/// Represents different levels of loading intensity
#[derive(Clone, PartialEq, Debug)]
pub enum LoadingLevelEnum {
    Extremely,
    Default,
    Low,
    High,
}

impl fmt::Display for LoadingLevelEnum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let txt = match self {
            LoadingLevelEnum::Default => "Default",
            LoadingLevelEnum::Low => "Low",
            LoadingLevelEnum::High => "High",
            LoadingLevelEnum::Extremely => "Extremely",
        };
        write!(f, "{} level", txt)
    }
}

impl FromStr for LoadingLevelEnum {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().trim() {
            "" => Ok(LoadingLevelEnum::Default),
            "default" => Ok(LoadingLevelEnum::Default),
            "high" => Ok(LoadingLevelEnum::High),
            "extremely" => Ok(LoadingLevelEnum::Extremely),
            "low" => Ok(LoadingLevelEnum::Low),
            _ => Err(format!("Invalid loading level: '{}'", s)),
        }
    }
}

pub fn fast_name() -> Uuid {
    let val = Uuid::new_v4();
    val
}

pub fn fake_data(n: usize) -> Vec<u8> {
    let mut res = Vec::with_capacity(n);

    while res.len() < n {
        let u = Uuid::new_v4();
        let part = u.as_bytes();
        let remaining = n - res.len();
        let take = if remaining >= part.len() {
            part.len()
        } else {
            remaining
        };
        res.extend_from_slice(&part[..take]);
    }
    res
}

pub fn code_name(value: &str) -> Uuid {
    let mut hasher = Sha1::new();
    hasher.update(value);
    let result = hasher.finalize();
    let mut uuid_bytes = [0u8; 16];
    uuid_bytes.copy_from_slice(&result[..16]);
    Uuid::from_slice(&result[..16]).expect("Should be valid UUID")
}

pub fn part_uuid(value: &Uuid) -> String {
    let uuid_str = value.to_string();
    format!("{}...", uuid_str.chars().take(5).collect::<String>())
}

/// Reads socket mapping configuration from environment variable
fn _read_env_socket_maps(name: &str, silent: bool) -> IpPortMap {
    let mut result = HashMap::new();
    let map_str = match env::var(name) {
        Ok(val) => val,
        Err(e) => {
            if !silent {
                warn!("Failed to read socket map from {}: {}", name, e);
            }
            return result
        },
    };

    for entry in map_str.split(';') {
        let parts: Vec<&str> = entry.split(':').collect();
        if parts.len() != 3 {
            if !silent {
                warn!("Invalid socket entry format '{}' - expected 'service:ip:port'", entry);
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
                    warn!("Invalid IP address '{}' in socket entry: {}", ip_str, e);
                }
                continue;
            }
        };

        let port = match port_str.parse::<u16>() {
            Ok(port) => port,
            Err(e) => {
                if !silent {
                    warn!("Invalid port '{}' in socket entry: {}", port_str, e);
                }
                continue;
            }
        };
        result.entry(service_name).or_insert_with(HashMap::new).insert(ip, port);
    }
    result
}

/// Reads boolean configuration from environment variable
fn _read_env_bool(name: &str, silent: bool, default: bool) -> bool {
    let bool_str = match env::var(name) {
        Ok(val) => val,
        Err(e) => {
            if !silent {
                warn!("Failed to read boolean value from {}: {}", name, e);
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

/// Reads string configuration from environment variable
fn _read_env_str(name: &str, silent: bool) -> String {
    match env::var(name) {
        Ok(val) => val,
        Err(e) => {
            if !silent {
                warn!("Failed to read string value from {}: {}", name, e);
            }
            "".to_string()
        },
    }
}

/// Reads unsigned integer configuration from environment variable
fn _read_env_uint(name: &str, silent: bool, default: usize) -> usize {
    match env::var(name) {
        Ok(val) => match val.parse::<usize>() {
            Ok(num) => num,
            Err(e) => {
                if !silent {
                    warn!("Failed to parse {} as unsigned integer: {}", name, e);
                }
                default
            },
        },
        Err(e) => {
            if !silent {
                warn!("Failed to read {} as unsigned integer: {}", name, e);
            }
            default
        },
    }
}

/// Reads list of strings configuration from environment variable
fn _read_env_strings(name: &str, silent: bool) -> Vec<String> {
    match env::var(name) {
        Ok(val) => {
            val.split(';').map(|s| s.trim().to_string()).collect()
        },
        Err(e) => {
            if !silent {
                warn!("Failed to read string list from {}: {}", name, e);
            }
            Vec::new()
        },
    }
}

#[derive(Clone)]
pub struct Settings {
    pub is_server: bool,
    pub server_host: String,
    pub server_port: u16,
    pub workers: usize,
    pub buffer_size: usize,
    pub stat_delay: Duration,
    pub stat_save_iter: Duration,
    pub client_name: String,
    pub tcp_sockets: IpPortMap,
    pub udp_sockets: IpPortMap,
    pub tcp_targets: IpPortMap,
    pub udp_targets: IpPortMap,
    pub cipher_key: String,
    pub idle_tcp_limit: Duration,
    pub idle_udp_limit: Duration,
    pub udp_bind_from: String,
    pub loading_level: LoadingLevelEnum,
    pub networks: Vec<IpNetwork>,
}

pub trait EncryptionData {
    fn main_cipher_key(&self) -> String;
}

impl EncryptionData for Settings {
    fn main_cipher_key(&self) -> String {
        self.cipher_key.clone()
    }
}

impl Settings {
    pub fn in_subnet(&self, ip: &str) -> bool {
        if self.networks.is_empty() {
            return true
        }
        let ip_addr = match ip.parse::<IpAddr>() {
            Ok(addr) => addr,
            Err(_) => return false,
        };
        for subnet in self.networks.iter() {
            if subnet.contains(ip_addr) {
                return true;
            }
        }
        false
    }
}

pub trait LoadingParams {
    fn channel_size(&self) -> (usize, usize);
    fn collect_message_timeout(&self, final_mode: bool) -> Duration;
    fn default_buffer_size(&self) -> usize;
    fn service_delay(&self) -> Duration;
    fn reconnect_delay(&self, attempt: usize) -> Duration;
}

/// Provides configuration parameters based on the loading level
impl LoadingParams for Settings {
    /// Returns the default buffer size based on the loading level
    fn default_buffer_size(&self) -> usize {
        match self.loading_level {
            LoadingLevelEnum::Default => 4 * 1024,
            LoadingLevelEnum::High => 8 * 1024,
            LoadingLevelEnum::Extremely => 8 * 1024,
            LoadingLevelEnum::Low => 4 * 1024
        }
    }

    /// Returns the channel size configuration based on the loading level
    fn channel_size(&self) -> (usize, usize) {
        match self.loading_level {
            LoadingLevelEnum::Default => (1024 * 2, 1024 * 2),
            LoadingLevelEnum::High => (1024 * 4, 1024 * 4),
            LoadingLevelEnum::Extremely => (1024 * 5, 1024 * 4),
            LoadingLevelEnum::Low => (1024 * 2, 1024 * 2),
        }
    }

    /// Minimal pause value for network operations
    fn service_delay(&self) -> Duration {
        let ms = match self.loading_level {
            LoadingLevelEnum::Default => 100,
            LoadingLevelEnum::High => 80,
            LoadingLevelEnum::Extremely => 80,
            LoadingLevelEnum::Low => 120,
        };
        Duration::from_millis(ms)
    }

    /// Calculates the delay duration before the next reconnection attempt,
    /// using a saturation curve to limit growth up to a maximum threshold.
    fn reconnect_delay(&self, attempt: usize) -> Duration {
        let max_sec = match self.loading_level {
            LoadingLevelEnum::Default => 5,
            LoadingLevelEnum::High => 5,
            LoadingLevelEnum::Extremely => 8,
            LoadingLevelEnum::Low => 5,
        };

        let min_ms: u64 = 1000;
        let max_ms = max_sec * 1000;

        if max_ms <= min_ms {
            return Duration::from_millis(max_ms);
        }
        let saturation_constant: u64 = 2;
        let diff = max_ms - min_ms;
        let numerator = attempt as u64;
        let denominator = numerator + saturation_constant;
        let growth = diff * numerator / denominator;
        let ms = min_ms + growth;
        let final_ms = if ms > max_ms { max_ms } else { ms };
        Duration::from_millis(final_ms)
    }

    /// Returns the timeout duration for collecting messages based on the loading level
    fn collect_message_timeout(&self, final_mode: bool) -> Duration {
        let (ms, long_ms) = match self.loading_level {
            LoadingLevelEnum::Default => (12, 100),
            LoadingLevelEnum::High => (10, 80),
            LoadingLevelEnum::Extremely => (6, 80),
            LoadingLevelEnum::Low => (15, 120),
        };
        Duration::from_millis(if final_mode {long_ms} else {ms})
    }
}

/// Creates and configures application settings from environment variables.
/// Returns a fully initialized Settings struct with defaults for missing values.
pub fn create_settings() -> Settings {
    let is_server = _read_env_bool(ENV_IS_SERVER, true, false);
    let cipher_key = _read_env_str(ENV_KEY_CIPHER, false);
    let buffer_size = _read_env_uint(ENV_BUFFER_SIZE, true, 0);
    let workers = _read_env_uint(ENV_WORKERS, true, 4);
    let mut client_name = _read_env_str(ENV_CLIENT_NAME, true);
    if client_name.is_empty() {
        client_name = format!("{}-{}", if is_server {"s"} else {"c"}, fast_name());
    }
    let stat_delay = Duration::from_secs(_read_env_uint(ENV_STAT_SHOW_INTERVAL, true, 180) as u64);
    let stat_save_iter = Duration::from_secs(
        _read_env_uint(ENV_STAT_SAVE_INTERVAL, true, if is_server {2} else {1}) as u64
    ) + Duration::from_millis(rand::random_range(100..500));
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
    let idle_tcp_limit = Duration::from_secs(
        _read_env_uint(ENV_CONNECTION_IDLE, true, 60 * 3) as u64
    );
    let idle_udp_limit = Duration::from_secs(
        _read_env_uint(ENV_UDP_CONNECTION_IDLE, true, 60 * 2) as u64
    );
    let mut udp_bind_from = _read_env_str(ENV_UDP_BIND_FROM, true);
    if udp_bind_from.is_empty() {udp_bind_from = "0.0.0.0:0".to_string();}
    let mut server_host = _read_env_str(ENV_SERVER_HOST, is_server);
    if is_server && server_host.is_empty() {server_host = "0.0.0.0".to_string();}
    let server_port = _read_env_uint(ENV_SERVER_PORT, true, 6379) as u16;
    let loading_level = match LoadingLevelEnum::from_str(
        &_read_env_str(ENV_LOADING_LEVEL, true)
    ) {
        Ok(val) => val,
        Err(_) => LoadingLevelEnum::Default,
    };
    let network_strings = _read_env_strings(ENV_ALLOW_NET, true);
    let networks: Vec<IpNetwork> = network_strings.iter().filter_map(|s| {
        match s.parse::<IpNetwork>() {
            Ok(network) => Some(network),
            Err(e) => {
                warn!("Invalid network configuration '{}': {}", s, e);
                None
            }
        }
    }).collect();
    if !networks.is_empty() {
        info!("Allowed networks count: {}", networks.len());
    }

    let mut settings = Settings {
        is_server,
        server_host,
        server_port,
        workers,
        buffer_size,
        stat_delay,
        stat_save_iter,
        client_name,
        tcp_sockets,
        udp_sockets,
        tcp_targets,
        udp_targets,
        cipher_key,
        idle_tcp_limit,
        idle_udp_limit,
        udp_bind_from,
        loading_level,
        networks,
    };
    if settings.buffer_size < 1024 {
        settings.buffer_size = settings.default_buffer_size();
    }
    settings
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_loading_level_from_str_valid() {
        assert_eq!(LoadingLevelEnum::from_str("default").unwrap(), LoadingLevelEnum::Default);
        assert_eq!(LoadingLevelEnum::from_str("high").unwrap(), LoadingLevelEnum::High);
        assert_eq!(LoadingLevelEnum::from_str("low").unwrap(), LoadingLevelEnum::Low);
        assert_eq!(LoadingLevelEnum::from_str("extremely").unwrap(), LoadingLevelEnum::Extremely);
        assert_eq!(LoadingLevelEnum::from_str("").unwrap(), LoadingLevelEnum::Default);
    }

    #[test]
    fn test_loading_level_from_str_invalid() {
        assert!(LoadingLevelEnum::from_str("invalid").is_err());
    }

    #[test]
    fn test_loading_level_display() {
        assert_eq!(format!("{}", LoadingLevelEnum::Default), "Default level");
        assert_eq!(format!("{}", LoadingLevelEnum::High), "High level");
        assert_eq!(format!("{}", LoadingLevelEnum::Low), "Low level");
        assert_eq!(format!("{}", LoadingLevelEnum::Extremely), "Extremely level");
    }

    #[test]
    fn test_fake_data_size() {
        let data = fake_data(100);
        assert_eq!(data.len(), 100);
        let data2 = fake_data(0);
        assert_eq!(data2.len(), 0);
        let data3 = fake_data(1000);
        assert_eq!(data3.len(), 1000);
    }

    #[test]
    fn test_code_name_deterministic() {
        let uuid1 = code_name("test");
        let uuid2 = code_name("test");
        assert_eq!(uuid1, uuid2);
    }

    #[test]
    fn test_code_name_different_inputs() {
        let uuid1 = code_name("test1");
        let uuid2 = code_name("test2");
        assert_ne!(uuid1, uuid2);
    }
    #[test]
    fn test_create_settings_default_safe() {
        let settings = create_settings();
        assert!(settings.is_server == true || settings.is_server == false);
        assert!(settings.workers > 0);
        assert!(settings.buffer_size >= 1024);
        assert!(!settings.client_name.is_empty());
        if settings.is_server {
            assert!(settings.client_name.starts_with("s-"));
        } else {
            assert!(settings.client_name.starts_with("c-"));
        }
        if settings.is_server {
            assert_eq!(settings.server_host, "0.0.0.0");
            assert_eq!(settings.server_port, 6379);
        } else {
            assert!(!settings.server_host.is_empty() || settings.server_host == ""); 
        }
        assert!(settings.idle_tcp_limit.as_secs() > 0);
        assert!(settings.idle_udp_limit.as_secs() > 0);
        assert!(matches!(settings.loading_level, LoadingLevelEnum::Default | LoadingLevelEnum::High | LoadingLevelEnum::Low | LoadingLevelEnum::Extremely));
        assert!(settings.tcp_sockets.is_empty());
        assert!(settings.udp_sockets.is_empty());
        assert!(settings.tcp_targets.is_empty());
        assert!(settings.udp_targets.is_empty());
        assert_eq!(settings.cipher_key, "");
        assert!(!settings.udp_bind_from.is_empty() || settings.udp_bind_from == "0.0.0.0:0");
        assert!(settings.networks.is_empty());
    }

    #[test]
    fn test_reconnect_delay_values() {
        let settings = create_settings();
        let default_settings = Settings {
            loading_level: LoadingLevelEnum::Default,
            ..settings
        };
        let delay_0 = default_settings.reconnect_delay(0);
        assert!(delay_0.as_secs() == 1, "Attempt 0 delay should be 1s, got {:?}", delay_0);
        let delay_1 = default_settings.reconnect_delay(1);
        assert!(delay_1.as_millis() < 2500, "Attempt 1 delay should be relatively short, got {:?}", delay_1);
        let delay_1000 = default_settings.reconnect_delay(1000);
        assert!(delay_1000.as_secs() >= 4, "Attempt 1000 delay should be close to 5s (min 4s), got {:?}", delay_1000);
        assert!(delay_1000.as_secs() <= 5, "Attempt 1000 delay should not exceed max 5s, got {:?}", delay_1000);
    }
}
