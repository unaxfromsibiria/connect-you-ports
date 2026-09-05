use std::fmt;
use std::str::FromStr;
use std::collections::HashMap;
use std::net::IpAddr;
use log::warn;
use uuid::Uuid;
use sha1::{Sha1, Digest};
use tokio::time::Duration;
use rand;

pub type IpPortMap = HashMap<String, HashMap<IpAddr, u16>>;

#[derive(PartialEq)]
pub enum TaskResultEnum {
    StopService,
    WorkerDone,
}

/// Represents different levels of loading intensity
#[derive(Clone, PartialEq, Debug)]
pub enum LoadingLevelEnum {
    Extremely,
    Default,
    Low,
    High,
}

#[derive(Clone, PartialEq, Debug)]
pub enum TransportTypeEnum {
    Mqtt,
    Http,
}

impl fmt::Display for TransportTypeEnum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let txt = match self {
            TransportTypeEnum::Mqtt => "MQTT",
            TransportTypeEnum::Http => "HTTP",
        };
        write!(f, "{}", txt)
    }
}

impl FromStr for TransportTypeEnum {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().trim() {
            "" => Ok(TransportTypeEnum::Mqtt),
            "mqtt" => Ok(TransportTypeEnum::Mqtt),
            "http" => Ok(TransportTypeEnum::Http),
            _ => Err(format!("Invalid transport type: '{}'", s)),
        }
    }
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

/// Reads socket mapping configuration from the text variable
fn _read_env_socket_maps(map_str: &str, silent: bool) -> IpPortMap {
    let mut result = HashMap::new();
    for entry in map_str.to_string().split(';') {
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

/// Short edition of settings for client without extra tuning
#[derive(Clone)]
pub struct Settings {
    pub server_host: String,
    pub server_port: u16,
    pub buffer_size: usize,
    pub stat_save_iter: Duration,
    pub client_name: String,
    pub tcp_sockets: IpPortMap,
    pub udp_sockets: IpPortMap,
    pub cipher_key: String,
    pub idle_tcp_limit: Duration,
    pub idle_udp_limit: Duration,
    pub loading_level: LoadingLevelEnum,
    pub verbose: bool,
    pub transport: TransportTypeEnum,
}

impl Settings {
    pub fn route_cleanup_interval(&self) -> Duration {
        self.idle_tcp_limit.max(self.idle_udp_limit)
    }
}

pub trait EncryptionData {
    fn main_cipher_key(&self) -> String;
    fn transport(&self) -> TransportTypeEnum;
}

impl EncryptionData for Settings {
    fn main_cipher_key(&self) -> String {
        self.cipher_key.clone()
    }
    fn transport(&self) -> TransportTypeEnum {
        self.transport.clone()
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
            LoadingLevelEnum::Default => (500, 500),
            LoadingLevelEnum::High => (700, 500),
            LoadingLevelEnum::Extremely => (1000, 800),
            LoadingLevelEnum::Low => (400, 400),
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
pub fn create_settings(server_host: &str, server_port: u16, key: &str, tcp_settings: &str, udp_settings: &str, verbose: bool, transport_str: &str) -> Settings {
    let client_name = format!("ph-{}", fast_name());
    let stat_save_iter = Duration::from_secs(120) + Duration::from_millis(rand::random_range(100..500));
    let tcp_sockets = _read_env_socket_maps(&tcp_settings, true);
    let udp_sockets = _read_env_socket_maps(&udp_settings, true);
    let idle_tcp_limit = Duration::from_secs(60 * 3);
    let idle_udp_limit = Duration::from_secs(60 * 2);
    let loading_level = LoadingLevelEnum::Extremely;
    let transport = match TransportTypeEnum::from_str(transport_str) {
        Ok(t) => t,
        Err(_) => TransportTypeEnum::Mqtt,
    };
    let mut settings = Settings {
        server_host: server_host.to_string(),
        server_port,
        buffer_size: 0,
        stat_save_iter,
        client_name,
        tcp_sockets,
        udp_sockets,
        cipher_key: key.to_string(),
        idle_tcp_limit,
        idle_udp_limit,
        loading_level,
        verbose,
        transport,
    };
    settings.buffer_size = settings.default_buffer_size();
    settings
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_loading_level_from_str() {
        assert_eq!(LoadingLevelEnum::from_str("default").unwrap(), LoadingLevelEnum::Default);
        assert_eq!(LoadingLevelEnum::from_str("High").unwrap(), LoadingLevelEnum::High);
        assert_eq!(LoadingLevelEnum::from_str("extremely").unwrap(), LoadingLevelEnum::Extremely);
        assert_eq!(LoadingLevelEnum::from_str("low").unwrap(), LoadingLevelEnum::Low);
        assert_eq!(LoadingLevelEnum::from_str("").unwrap(), LoadingLevelEnum::Default);
        assert!(LoadingLevelEnum::from_str("invalid").is_err());
    }

    #[test]
    fn test_loading_level_display() {
        assert_eq!(format!("{}", LoadingLevelEnum::Default), "Default level");
        assert_eq!(format!("{}", LoadingLevelEnum::High), "High level");
        assert_eq!(format!("{}", LoadingLevelEnum::Extremely), "Extremely level");
        assert_eq!(format!("{}", LoadingLevelEnum::Low), "Low level");
    }

    #[test]
    fn test_code_name_deterministic() {
        let name1 = code_name("test");
        let name2 = code_name("test");
        assert_eq!(name1, name2);
        let name3 = code_name("other");
        assert_ne!(name1, name3);
    }

    #[test]
    fn test_part_uuid() {
        let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let part = part_uuid(&uuid);
        assert_eq!(part, "550e8...");
    }

    #[test]
    fn test_read_env_socket_maps_valid() {
        let map_str = "service1:127.0.0.1:8080;service2:192.168.1.1:9090";
        let result = _read_env_socket_maps(map_str, true);
        assert_eq!(result.len(), 2);
        assert!(result.contains_key("service1"));
        assert!(result.contains_key("service2"));
        let service1_map = &result["service1"];
        assert_eq!(service1_map[&IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1))], 8080);
    }

    #[test]
    fn test_read_env_socket_maps_invalid() {
        let map_str = "bad_format;service:invalid_ip:80;service:127.0.0.1:99999";
        let result = _read_env_socket_maps(map_str, true);
        // All entries are invalid (wrong format, bad IP, port out of range)
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_settings_loading_params_default() {
        let settings = Settings {
            server_host: "localhost".to_string(),
            server_port: 8080,
            buffer_size: 1024,
            stat_save_iter: Duration::from_secs(60),
            client_name: "test".to_string(),
            tcp_sockets: HashMap::new(),
            udp_sockets: HashMap::new(),
            cipher_key: "key".to_string(),
            idle_tcp_limit: Duration::from_secs(180),
            idle_udp_limit: Duration::from_secs(120),
            loading_level: LoadingLevelEnum::Default,
            verbose: false,
            transport: TransportTypeEnum::Mqtt,
        };
        assert_eq!(settings.default_buffer_size(), 4 * 1024);
        assert_eq!(settings.channel_size(), (500, 500));
        assert_eq!(settings.service_delay(), Duration::from_millis(100));
    }

    #[test]
    fn test_settings_loading_params_extremely() {
        let settings = Settings {
            server_host: "localhost".to_string(),
            server_port: 8080,
            buffer_size: 1024,
            stat_save_iter: Duration::from_secs(60),
            client_name: "test".to_string(),
            tcp_sockets: HashMap::new(),
            udp_sockets: HashMap::new(),
            cipher_key: "key".to_string(),
            idle_tcp_limit: Duration::from_secs(180),
            idle_udp_limit: Duration::from_secs(120),
            loading_level: LoadingLevelEnum::Extremely,
            verbose: false,
            transport: TransportTypeEnum::Mqtt,
        };
        assert_eq!(settings.default_buffer_size(), 8 * 1024);
        assert_eq!(settings.channel_size(), (1000, 800));
        assert_eq!(settings.service_delay(), Duration::from_millis(80));
    }

    #[test]
    fn test_reconnect_delay_saturation() {
        let settings = Settings {
            server_host: "localhost".to_string(),
            server_port: 8080,
            buffer_size: 1024,
            stat_save_iter: Duration::from_secs(60),
            client_name: "test".to_string(),
            tcp_sockets: HashMap::new(),
            udp_sockets: HashMap::new(),
            cipher_key: "key".to_string(),
            idle_tcp_limit: Duration::from_secs(180),
            idle_udp_limit: Duration::from_secs(120),
            loading_level: LoadingLevelEnum::Default, // max_sec = 5
            verbose: false,
            transport: TransportTypeEnum::Mqtt,
        };
        let delay_0 = settings.reconnect_delay(0);
        assert_eq!(delay_0.as_millis(), 1000); // min_ms
        let delay_high = settings.reconnect_delay(100);
        assert!(delay_high.as_secs() <= 5); // Should not exceed max_sec        
        // Check that it increases with attempts but saturates
        let delay_1 = settings.reconnect_delay(1);
        let delay_2 = settings.reconnect_delay(2);
        assert!(delay_1 < delay_2);
    }

    #[test]
    fn test_create_settings() {
        let settings = create_settings("localhost", 8080, "secret_key", "", "", false, "");
        assert_eq!(settings.server_host, "localhost");
        assert_eq!(settings.server_port, 8080);
        assert_eq!(settings.cipher_key, "secret_key");
        assert_eq!(settings.loading_level, LoadingLevelEnum::Extremely);
        assert!(!settings.verbose);
        assert!(settings.client_name.starts_with("ph-"));
    }

    #[test]
    fn test_encryption_data_trait() {
        let settings = Settings {
            server_host: "localhost".to_string(),
            server_port: 8080,
            buffer_size: 1024,
            stat_save_iter: Duration::from_secs(60),
            client_name: "test".to_string(),
            tcp_sockets: HashMap::new(),
            udp_sockets: HashMap::new(),
            cipher_key: "my_secret".to_string(),
            idle_tcp_limit: Duration::from_secs(180),
            idle_udp_limit: Duration::from_secs(120),
            loading_level: LoadingLevelEnum::Default,
            verbose: false,
            transport: TransportTypeEnum::Mqtt,
        };

        assert_eq!(settings.main_cipher_key(), "my_secret");
    }
}
