use std::collections::BTreeMap;
use std::fs;

use crate::settings::{IpPortMap, Settings, code_name, part_uuid};
use crate::stat::{MEMORY_MODE, memory_content};

fn print_service_map(proto: &str, map: &IpPortMap) {
    if map.is_empty() {
        return;
    }
    let mut names: Vec<&String> = map.keys().collect();
    names.sort();
    for name in names {
        let code = part_uuid(&code_name(name));
        for (ip, port) in &map[name] {
            println!("  {} ({}) -> {}://{}:{}", name, code, proto, ip, port);
        }
    }
}

// Print service layout to stdout regardless of RUST_LOG level.
pub fn main_info(settings: &Settings) {
    if settings.is_server {
        println!(
            "Mode: server | listening on tcp://{}:{} for clients", settings.server_host, settings.server_port
        );
        let allowed = if settings.networks.is_empty() {
            "no restriction".to_string()
        } else {
            settings.networks.iter().map(|n| n.to_string()).collect::<Vec<_>>().join(", ")
        };
        println!("Allowed networks: {}", allowed);
    } else {
        println!(
            "Mode: client ({}) | will connect to tcp://{}:{}",
            settings.client_name,
            settings.server_host,
            settings.server_port,
        );
        println!("Services (local sockets that will be opened):");
        print_service_map("tcp", &settings.tcp_sockets);
        print_service_map("udp", &settings.udp_sockets);
    }
    println!(
        "Loading level: {} | stat update interval: {:.1}s",
        settings.loading_level,
        settings.stat_save_iter.as_secs_f64(),
    );
}

pub fn print_env_help() {
    let envs = [
        ("ALLOW_NET", "allowed networks list, CIDR entries separated by ';' (server mode); empty means no restriction"),
        ("CONNECTION_IDLE_LIMIT", "TCP connection idle timeout in seconds (default: 180)"),
        ("CRYPTO_KEY", "cipher key in hex, AES-256-GCM requires 32 bytes; generate with --genkey"),
        ("LOADING_LEVEL", "loading level: default, low, high, extremely (default: default)"),
        ("READ_BUFFER_SIZE", "read buffer size in bytes; below 1024 uses the loading-level default"),
        ("SERVER", "run as server when set to on/yes/true/ok/1, otherwise runs as a client"),
        ("SERVER_HOST", "listen address for the server, target host for the client (default: 0.0.0.0)"),
        ("SERVER_PORT", "server port (default: 1883)"),
        ("SERVER_TCP_TARGET", "TCP targets map 'name:ip:port;...' on the server side"),
        ("SERVER_UDP_TARGET", "UDP targets map 'name:ip:port;...' on the server side"),
        ("STAT_FILE", "statistics file path (default: /tmp/stat.txt); use 'memory' to keep stats in process memory instead of a file"),
        ("STAT_SAVE_INTERVAL", "stats save interval in seconds (server default: 2, client default: 1)"),
        ("STAT_SHOW_INTERVAL", "stats display interval in seconds (default: 180)"),
        ("TCP_SOCKETS", "local TCP sockets map 'name:ip:port;...' forwarded through the tunnel (client side)"),
        ("UDP_BIND_FROM", "source address for outbound UDP on the server side (default: 0.0.0.0:0)"),
        ("UDP_CONNECTION_IDLE_LIMIT", "UDP idle timeout in seconds (default: 120)"),
        ("UDP_SOCKETS", "local UDP sockets map 'name:ip:port;...' forwarded through the tunnel (client side)"),
        ("WORKERS", "tokio worker threads; 0 or unset means auto-detect CPU cores"),
    ];
    println!("Environment variables (alphabetical):");
    for (var, desc) in envs {
        println!("  {:<27} {}", var, desc);
    }
}

pub fn print_usage(bin_name: &str) {
    println!("{}", bin_name);
    println!();
    println!("Rust-based TCP tunnel client-server application for forwarding TCP and UDP traffic.");
    println!("A secure persistent TCP tunnel between a local client and a remote server can forward");
    println!("multiple named TCP connections and UDP streams to destinations on the other side,");
    println!("e.g. services in cloud infrastructure without direct access.");
    println!();
    println!("Usage: {} [OPTIONS]", bin_name);
    println!();
    println!("Options:");
    println!("  --server           run as server (without this flag the app runs as a client)");
    println!("  --workers <N>      tokio worker threads; 0 or unset means auto-detect CPU cores");
    println!("  --genkey [N]       generate a random cipher key of N bytes in hex and exit (default: 32, AES-256)");
    println!("  --stat             read statistics (file STAT_FILE, default /tmp/stat.txt; 'memory' reads the in-process buffer) and print it as a table");
    println!("  -h, --help         print this help message and exit");
    println!();
    print_env_help();
}

#[derive(Default)]
struct StatRow {
    in_traffic: Option<String>,
    out_traffic: Option<String>,
    conn_total: Option<String>,
    conn_lost: Option<String>,
    errors: Option<String>,
    metric: Option<String>,
}

fn split_marker<'a>(body: &'a str, marker: &str) -> Option<(&'a str, &'a str)> {
    let pos = body.rfind(marker)?;
    Some((&body[..pos], &body[pos + marker.len()..]))
}

// Parse the stat file format produced by Stat::format_stats into per-service rows.
fn parse_stat_content(content: &str) -> BTreeMap<String, StatRow> {
    let mut rows: BTreeMap<String, StatRow> = BTreeMap::new();

    enum Section { Traffic, Connections, Errors, Metrics }

    let mut section: Option<Section> = None;
    for line in content.lines() {
        if !line.starts_with(' ') {
            section = match line.trim() {
                "traffic:" => Some(Section::Traffic),
                "connections:" => Some(Section::Connections),
                "errors:" => Some(Section::Errors),
                "metrics:" => Some(Section::Metrics),
                _ => None,
            };
            continue;
        }
        let rest = line.trim();
        if !rest.starts_with("target ") {
            continue;
        }
        let body = &rest["target ".len()..];
        match section {
            Some(Section::Traffic) => {
                let Some((name, rest)) = split_marker(body, " in:") else { continue };
                let Some((in_val, out_val)) = split_marker(rest, " out:") else { continue };
                let row = rows.entry(name.trim().to_string()).or_default();
                row.in_traffic = Some(in_val.trim().to_string());
                row.out_traffic = Some(out_val.trim().to_string());
            }
            Some(Section::Connections) => {
                let Some((name, rest)) = split_marker(body, " total:") else { continue };
                let Some((total, lost)) = split_marker(rest, " lost:") else { continue };
                let row = rows.entry(name.trim().to_string()).or_default();
                row.conn_total = Some(total.trim().to_string());
                row.conn_lost = Some(lost.trim().to_string());
            }
            Some(Section::Errors) => {
                let Some((name, count)) = split_marker(body, " errors:") else { continue };
                rows.entry(name.trim().to_string()).or_default().errors = Some(count.trim().to_string());
            }
            Some(Section::Metrics) => {
                let Some((name, value)) = split_marker(body, " value:") else { continue };
                rows.entry(name.trim().to_string()).or_default().metric = Some(value.trim().to_string());
            }
            None => {}
        }
    }
    rows
}

fn build_stat_table(content: &str) -> String {
    let rows = parse_stat_content(content);
    if rows.is_empty() {
        return "(no statistics recorded)".to_string();
    }
    const HEADERS: [&str; 7] = ["service", "in", "out", "total conns", "lost conns", "errors", "metric"];

    let data: Vec<Vec<String>> = rows.iter().map(|(name, row)| {
        vec![
            name.clone(),
            cell(&row.in_traffic),
            cell(&row.out_traffic),
            cell(&row.conn_total),
            cell(&row.conn_lost),
            cell(&row.errors),
            cell(&row.metric),
        ]
    }).collect();

    let mut widths: Vec<usize> = HEADERS.iter().map(|h| h.chars().count()).collect();
    for r in &data {
        for (i, c) in r.iter().enumerate() {
            widths[i] = widths[i].max(c.chars().count());
        }
    }

    let mut out = String::new();
    out.push_str(&HEADERS.iter().enumerate().map(
        |(i, h)| format!("{:<w$}", h, w = widths[i])
    ).collect::<Vec<_>>().join("  "));
    out.push('\n');
    out.push_str(&widths.iter().map(|w| "-".repeat(*w)).collect::<Vec<_>>().join("  "));
    for r in &data {
        out.push('\n');
        out.push_str(&r.iter().enumerate().map(
            |(i, c)| format!("{:<w$}", c, w = widths[i])
        ).collect::<Vec<_>>().join("  "));
    }
    out
}

fn cell(value: &Option<String>) -> String {
    value.clone().unwrap_or_else(|| "-".to_string())
}

pub fn show_stat_table(filepath: &str) -> Result<(), String> {
    let content = if filepath == MEMORY_MODE {
        memory_content()
    } else {
        fs::read_to_string(filepath).map_err(
            |e| format!("failed to read statistics file '{}': {}", filepath, e)
        )?
    };
    println!("{}", build_stat_table(&content));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = "\
traffic:
  target db in: 123.4 kb out: 567.8 mb
  target api in: 10.0 kb out: 2.5 kb
connections:
  target db total: 12 lost: 1
errors:
  target db errors: 7
metrics:
  target api value: 99
";

    #[test]
    fn test_parse_stat_content_sorted() {
        let rows = parse_stat_content(SAMPLE);
        assert_eq!(rows.keys().collect::<Vec<_>>(), vec!["api", "db"]);
        assert_eq!(rows.get("db").unwrap().in_traffic.as_deref(), Some("123.4 kb"));
        assert_eq!(rows.get("db").unwrap().out_traffic.as_deref(), Some("567.8 mb"));
        assert_eq!(rows.get("db").unwrap().conn_total.as_deref(), Some("12"));
        assert_eq!(rows.get("api").unwrap().metric.as_deref(), Some("99"));
    }

    #[test]
    fn test_build_stat_table() {
        let table = build_stat_table(SAMPLE);
        assert!(table.contains("service"));
        assert!(table.contains("-----"));
        assert!(table.contains("123.4 kb"));
        assert!(table.contains("567.8 mb"));
    }

    #[test]
    fn test_build_stat_table_empty() {
        assert_eq!(build_stat_table(""), "(no statistics recorded)");
    }
}
