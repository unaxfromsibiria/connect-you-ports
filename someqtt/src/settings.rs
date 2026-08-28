use ipnetwork::IpNetwork;
use log::{info, warn};
use rand;
use sha1::{Digest, Sha1};
use std::collections::HashMap;
use std::env;
use std::fmt;
use std::net::IpAddr;
use std::str::FromStr;
use tokio::time::Duration;
use uuid::Uuid;

// env variables
const ENV_IS_SERVER: &str = "SERVER";
const ENV_WORKERS: &str = "WORKERS";
const ENV_BUFFER_SIZE: &str = "READ_BUFFER_SIZE";
const ENV_TCP_SOCKETS: &str = "TCP_SOCKETS";
const ENV_UDP_SOCKETS: &str = "UDP_SOCKETS";
const ENV_STAT_SHOW_INTERVAL: &str = "STAT_SHOW_INTERVAL";
const ENV_STAT_FILE: &str = "STAT_FILE";
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

pub const DEFAULT_STAT_FILEPATH: &str = "/tmp/stat.txt";
pub type IpPortMap = HashMap<String, HashMap<IpAddr, u16>>;
pub const OUT_TTL: u32 = 96;

#[derive(Clone, PartialEq, Debug)]
pub enum LoadingLevelEnum {
    Extremely,
    Default,
    Low,
    High,
}

#[derive(PartialEq, Debug)]
pub enum TaskResultEnum {
    StopService,
    WorkerDone,
    WrongSettings,
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

const ENV_CLIENT_NAME: &str = "CLIENT_NAME";

fn generate_client_name() -> Uuid {
    // explicit value, then OS host name, otherwise random
    if let Ok(val) = env::var(ENV_CLIENT_NAME) && !val.trim().is_empty() {
        return code_name(&val);
    }
    if let Ok(name) = gethostname::gethostname().into_string() && !name.trim().is_empty() {
        return code_name(&name);
    }
    fast_name()
}

pub fn fast_name() -> Uuid {
    Uuid::new_v4()
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
    pub stat_filepath: String,
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
    pub client_name: Uuid
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
    pub fn route_cleanup_interval(&self) -> Duration {
        self.idle_tcp_limit.max(self.idle_udp_limit)
    }

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
    fn drain_total(&self) -> Duration;
}

impl LoadingParams for Settings {
    fn default_buffer_size(&self) -> usize {
        match self.loading_level {
            LoadingLevelEnum::Default => 4 * 1024,
            LoadingLevelEnum::High => 8 * 1024,
            LoadingLevelEnum::Extremely => 8 * 1024,
            LoadingLevelEnum::Low => 4 * 1024
        }
    }

    fn channel_size(&self) -> (usize, usize) {
        match self.loading_level {
            LoadingLevelEnum::Default => (500, 500),
            LoadingLevelEnum::High => (800, 600),
            LoadingLevelEnum::Extremely => (1000, 800),
            LoadingLevelEnum::Low => (400, 400),
        }
    }

    fn service_delay(&self) -> Duration {
        let ms = match self.loading_level {
            LoadingLevelEnum::Default => 100,
            LoadingLevelEnum::High => 80,
            LoadingLevelEnum::Extremely => 80,
            LoadingLevelEnum::Low => 120,
        };
        Duration::from_millis(ms)
    }

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

    fn collect_message_timeout(&self, final_mode: bool) -> Duration {
        let (ms, long_ms) = match self.loading_level {
            LoadingLevelEnum::Default => (12, 100),
            LoadingLevelEnum::High => (10, 80),
            LoadingLevelEnum::Extremely => (6, 80),
            LoadingLevelEnum::Low => (15, 120),
        };
        Duration::from_millis(if final_mode {long_ms} else {ms})
    }

    fn drain_total(&self) -> Duration {
        let ms = match self.loading_level {
            LoadingLevelEnum::Default => 1000,
            LoadingLevelEnum::High => 500,
            LoadingLevelEnum::Extremely => 300,
            LoadingLevelEnum::Low => 2000,
        };
        Duration::from_millis(ms)
    }
}

#[derive(Clone, Copy, Default)]
pub struct CliOverrides {
    pub is_server: Option<bool>,
    pub workers: Option<usize>,
    pub genkey: Option<usize>,
    pub stat: bool,
}

pub enum CliParseResult {
    Help,
    Args(CliOverrides),
}

fn parse_bool_value(value: &str) -> bool {
    let normalized = value.to_lowercase();
    let key = normalized.trim();
    ["on", "yes", "1", "true", "ok"].contains(&key)
}

pub fn parse_cli_args(args: &[String]) -> Result<CliParseResult, String> {
    let mut overrides = CliOverrides::default();
    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if arg == "-h" || arg == "--help" {
            return Ok(CliParseResult::Help);
        }
        if arg == "--" {
            break;
        }
        if !arg.starts_with("--") {
            return Err(format!("unexpected argument: {}", arg));
        }
        let rest = &arg[2..];
        let (name, inline) = match rest.split_once('=') {
            Some((n, v)) => (n, Some(v.to_string())),
            None => (rest, None),
        };
        match name {
            "server" => {
                overrides.is_server = Some(match inline {
                    Some(v) => parse_bool_value(&v),
                    None => true,
                });
            }
            "genkey" => {
                let raw = if let Some(v) = inline {
                    v
                } else if i + 1 < args.len() && !args[i + 1].starts_with('-') {
                    // Only a bare numeric argument is taken as the size value.
                    match args[i + 1].parse::<usize>() {
                        Ok(_) => { let v = args[i + 1].clone(); i += 1; v }
                        Err(e) => return Err(format!("invalid value for --genkey: '{}'", e)),
                    }
                } else {
                    "32".to_string()
                };
                let size: usize = raw.parse().map_err(|_| format!("invalid value for --genkey: '{}'", raw))?;
                if size < 1 || size > 512 {
                    return Err(format!("--genkey length must be between 1 and 512 bytes, got {}", size));
                }
                overrides.genkey = Some(size);
            }
            "stat" => {
                overrides.stat = match inline {
                    Some(v) => parse_bool_value(&v),
                    None => true,
                };
            }
            "workers" => {
                let raw = if let Some(v) = inline {
                    v
                } else if i + 1 < args.len() {
                    i += 1;
                    args[i].clone()
                } else {
                    return Err(format!("missing value for --{}", name));
                };
                let num: usize = raw.parse().map_err(|_| format!("invalid value for --{}: '{}'", name, raw))?;
                if name == "workers" {
                    overrides.workers = Some(num);
                }
            }
            _ => return Err(format!("unknown argument: --{}", name)),
        }
        i += 1;
    }
    Ok(CliParseResult::Args(overrides))
}

pub fn generate_cipher_key(size: usize) -> String {
    let mut buf = vec![0u8; size];
    rand::fill(&mut buf);
    hex::encode(buf)
}

pub fn default_stat_filepath() -> String {
    match _read_env_str(ENV_STAT_FILE, true) {
        path if !path.is_empty() => path,
        _ => DEFAULT_STAT_FILEPATH.to_string(),
    }
}

pub fn create_settings(overrides: &CliOverrides) -> Settings {
    let is_server = overrides.is_server.unwrap_or_else(|| _read_env_bool(ENV_IS_SERVER, true, false));
    let cipher_key = _read_env_str(ENV_KEY_CIPHER, false);
    let buffer_size = _read_env_uint(ENV_BUFFER_SIZE, true, 0);
    let workers = overrides.workers.unwrap_or_else(|| _read_env_uint(ENV_WORKERS, true, 0));
    // Added reading for stat_filepath with default value
    let stat_filepath = default_stat_filepath();
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
    let server_port = _read_env_uint(ENV_SERVER_PORT, true, 1883) as u16;
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
        stat_filepath,
        client_name: generate_client_name(),
    };
    if settings.buffer_size < 1024 {
        settings.buffer_size = settings.default_buffer_size();
    }
    settings
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_loading_level_from_str() {
        assert_eq!(LoadingLevelEnum::from_str("").unwrap(), LoadingLevelEnum::Default);
        assert_eq!(LoadingLevelEnum::from_str("default").unwrap(), LoadingLevelEnum::Default);
        assert_eq!(LoadingLevelEnum::from_str("HIGH").unwrap(), LoadingLevelEnum::High);
        assert_eq!(LoadingLevelEnum::from_str("extremely").unwrap(), LoadingLevelEnum::Extremely);
        assert_eq!(LoadingLevelEnum::from_str("low").unwrap(), LoadingLevelEnum::Low);
    }

    #[test]
    fn test_loading_level_display() {
        assert_eq!(format!("{}", LoadingLevelEnum::Default), "Default level");
        assert_eq!(format!("{}", LoadingLevelEnum::High), "High level");
    }

    #[test]
    fn test_part_uuid() {
        let uuid = Uuid::parse_str("12345678-1234-5678-1234-567812345678").unwrap();
        let part = part_uuid(&uuid);
        assert!(part.starts_with("12345"));
        assert!(part.ends_with("..."));
    }

    #[test]
    fn test_code_name_deterministic() {
        let u1 = code_name("test");
        let u2 = code_name("test");
        assert_eq!(u1, u2);
    }

    #[test]
    fn test_cli_help_flags() {
        for flag in ["--help", "-h"] {
            match parse_cli_args(&[flag.to_string()]) {
                Ok(CliParseResult::Help) => {}
                _ => panic!("expected Help for {}", flag),
            }
        }
    }

    #[test]
    fn test_cli_server_flag() {
        let o = match parse_cli_args(&["--server".to_string()]) {
            Ok(CliParseResult::Args(o)) => o,
            _ => panic!("expected Args"),
        };
        assert_eq!(o.is_server, Some(true));

        let o = match parse_cli_args(&["--server=yes".to_string()]) {
            Ok(CliParseResult::Args(o)) => o,
            _ => panic!("expected Args"),
        };
        assert_eq!(o.is_server, Some(true));

        let o = match parse_cli_args(&["--server=0".to_string()]) {
            Ok(CliParseResult::Args(o)) => o,
            _ => panic!("expected Args"),
        };
        assert_eq!(o.is_server, Some(false));
    }

    #[test]
    fn test_cli_unknown_or_extra_args_rejected() {
        assert!(parse_cli_args(&["--unknown".to_string()]).is_err());
        assert!(parse_cli_args(&["positional".to_string()]).is_err());
    }

    #[test]
    fn test_cli_no_flags_gives_empty_overrides() {
        match parse_cli_args(&[]) {
            Ok(CliParseResult::Args(o)) => {
                assert_eq!(o.is_server, None);
                assert_eq!(o.workers, None);
            }
            _ => panic!("expected Args"),
        }
    }

    #[test]
    fn test_cli_genkey() {
        let o = match parse_cli_args(&["--genkey".to_string()]) {
            Ok(CliParseResult::Args(o)) => o,
            _ => panic!("expected Args"),
        };
        assert_eq!(o.genkey, Some(32));

        let o = match parse_cli_args(&["--genkey".to_string(), "16".to_string()]) {
            Ok(CliParseResult::Args(o)) => o,
            _ => panic!("expected Args"),
        };
        assert_eq!(o.genkey, Some(16));

        let o = match parse_cli_args(&["--genkey=48".to_string()]) {
            Ok(CliParseResult::Args(o)) => o,
            _ => panic!("expected Args"),
        };
        assert_eq!(o.genkey, Some(48));
        // non-numeric size or out-of-range is an error
        assert!(parse_cli_args(&["--genkey=abc".to_string()]).is_err());
        assert!(parse_cli_args(&["--genkey".to_string(), "0".to_string()]).is_err());
    }

    #[test]
    fn test_cli_stat_flag() {
        let o = match parse_cli_args(&["--stat".to_string()]) {
            Ok(CliParseResult::Args(o)) => o,
            _ => panic!("expected Args"),
        };
        assert!(o.stat);

        let o = match parse_cli_args(&["--stat=0".to_string()]) {
            Ok(CliParseResult::Args(o)) => o,
            _ => panic!("expected Args"),
        };
        assert!(!o.stat);
    }

    #[test]
    fn test_default_stat_filepath() {
        assert_eq!(default_stat_filepath(), DEFAULT_STAT_FILEPATH.to_string());
    }

    #[test]
    fn test_generate_cipher_key_format() {
        for size in [16usize, 32usize, 48usize] {
            let key = generate_cipher_key(size);
            assert_eq!(key.len(), size * 2);
            let decoded: Vec<u8> = hex::decode(&key).expect("valid hex");
            assert_eq!(decoded.len(), size);
        }
        // two keys should differ
        assert_ne!(generate_cipher_key(32), generate_cipher_key(32));
    }

    #[test]
    fn test_in_subnet() {
        let networks: Vec<IpNetwork> = vec![
            "10.0.0.0/8".parse().unwrap(),
            "192.168.1.5/32".parse().unwrap(),
        ];

        // empty allow-list permits everything (no restriction)
        let empty = Settings {networks: Vec::new(), ..default_test_settings()};
        assert!(empty.in_subnet("8.8.8.8"));

        let s = Settings {networks, ..default_test_settings()};
        assert!(s.in_subnet("10.255.255.255"), "in /8 subnet");
        assert!(!s.in_subnet("11.0.0.1"), "outside /8 and not the exact host");
        assert!(s.in_subnet("192.168.1.5"), "exact /32 match");
        assert!(!s.in_subnet("192.168.1.6"), "not in /32");
        assert!(!s.in_subnet("not-an-ip"), "unparseable ip is rejected when list set");
    }

    fn default_test_settings() -> Settings {
        Settings {
            is_server: true,
            server_host: String::new(),
            server_port: 0,
            workers: 0,
            buffer_size: 1024,
            stat_delay: Duration::from_secs(0),
            stat_save_iter: Duration::from_secs(0),
            stat_filepath: String::new(),
            tcp_sockets: IpPortMap::default(),
            udp_sockets: IpPortMap::default(),
            tcp_targets: IpPortMap::default(),
            udp_targets: IpPortMap::default(),
            cipher_key: String::new(),
            idle_tcp_limit: Duration::from_secs(0),
            idle_udp_limit: Duration::from_secs(0),
            udp_bind_from: String::new(),
            loading_level: LoadingLevelEnum::Default,
            networks: Vec::new(),
            client_name: fast_name(),
        }
    }
}
