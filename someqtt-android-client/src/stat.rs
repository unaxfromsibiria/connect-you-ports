use tokio::sync::RwLock;
use std::collections::HashMap;
use once_cell::sync::Lazy;

struct Stat {
    // { service_name: (incoming, outgoing) }
    traffic: HashMap<String, (usize, usize)>,
    // { service_name: (current_connections, total_connections) }
    connections: HashMap<String, (usize, usize)>,
    // { service_name: count }
    errors: HashMap<String, usize>,
    metrics: HashMap<String, usize>,
}

impl Stat {
    fn new() -> Self {
        Self {
            traffic: HashMap::new(),
            connections: HashMap::new(),
            errors: HashMap::new(),
            metrics: HashMap::new(),
        }
    }
}

static GLOBAL_STAT: Lazy<RwLock<Stat>> = Lazy::new(|| {
    RwLock::new(Stat::new())
});

pub async fn add_connection(service: &str) {
    let mut stat = GLOBAL_STAT.write().await;
    let entry = stat.connections.entry(service.to_string()).or_insert((0, 0));
    entry.0 += 1;
}

pub async fn lost_connection(service: &str) {
    let mut stat = GLOBAL_STAT.write().await;
    if let Some(conn) = stat.connections.get_mut(service) {
        conn.1 += 1;
    }
}

pub async fn update_metric(service: &str, value: usize) {
    let mut stat = GLOBAL_STAT.write().await;
    stat.metrics.insert(service.to_string(), value);
}

pub async fn update_traffic_stats(service: &str, in_bytes: usize, out_bytes: usize, errors: usize) {
    let mut stat = GLOBAL_STAT.write().await;
    let traffic_entry = stat.traffic.entry(service.to_string()).or_insert((0, 0));
    traffic_entry.0 += in_bytes;
    traffic_entry.1 += out_bytes;
    if errors > 0 {
        let error_count = stat.errors.entry(service.to_string()).or_insert(0);
        *error_count += errors;
    }
}

pub fn show_stats_sync() -> String {
    let stat = GLOBAL_STAT.blocking_read();
    build_stat_table(&stat)
}

fn build_stat_table(stat: &Stat) -> String {
    // collect all service names
    use std::collections::BTreeSet;
    let mut services = BTreeSet::new();
    for k in stat.traffic.keys() { services.insert(k.clone()); }
    for k in stat.connections.keys() { services.insert(k.clone()); }
    for k in stat.errors.keys() { services.insert(k.clone()); }
    for k in stat.metrics.keys() { services.insert(k.clone()); }

    if services.is_empty() {
        return "(no statistics recorded)".to_string();
    }

    const HEADERS: [&str; 7] = ["service", "in", "out", "total conns", "lost conns", "errors", "metric"];

    let data: Vec<Vec<String>> = services.iter().map(|name| {
        let in_out = stat.traffic.get(name).cloned().unwrap_or((0,0));
        let (in_val_str, out_val_str) = format_traffic(in_out.0, in_out.1);
        let (total_conns, lost_conns) = stat.connections.get(name).cloned().unwrap_or((0,0));
        let errors = stat.errors.get(name).cloned().unwrap_or(0).to_string();
        let metric = stat.metrics.get(name).cloned().unwrap_or(0).to_string();
        vec![
            name.clone(),
            in_val_str,
            out_val_str,
            total_conns.to_string(),
            lost_conns.to_string(),
            errors,
            metric,
        ]
    }).collect();

    let mut widths: Vec<usize> = HEADERS.iter().map(|h| h.chars().count()).collect();
    for r in &data {
        for (i, c) in r.iter().enumerate() {
            widths[i] = widths[i].max(c.chars().count());
        }
    }

    let mut out = String::new();
    out.push_str(&HEADERS.iter().enumerate().map(|(i,h)| format!("{:<w$}", h, w=widths[i])).collect::<Vec<_>>().join("  "));
    out.push('\n');
    out.push_str(&widths.iter().map(|w| "-".repeat(*w)).collect::<Vec<_>>().join("  "));
    for r in &data {
        out.push('\n');
        out.push_str(&r.iter().enumerate().map(|(i,c)| format!("{:<w$}", c, w=widths[i])).collect::<Vec<_>>().join("  "));
    }
    out
}

fn format_traffic(in_bytes: usize, out_bytes: usize) -> (String, String) {
    let fmt = |v: usize| -> String {
        if v >= 1024 * 1024 {
            let mb = (v as f64 / (1024.0 * 1024.0)) * 10.0;
            format!("{:.1} mb", mb.round() / 10.0)
        } else {
            let kb = (v as f64 / 1024.0) * 10.0;
            format!("{:.1} kb", kb.round() / 10.0)
        }
    };
    (fmt(in_bytes), fmt(out_bytes))
}


#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_add_connection() {
        let key = "test_add_conn_unique_1";
        add_connection(key).await;
        let stat = GLOBAL_STAT.read().await;
        assert_eq!(stat.connections.get(key), Some(&(1, 0)));
    }

    #[tokio::test]
    async fn test_lost_connection() {
        let key = "test_lost_conn_unique_2";
        add_connection(key).await;
        lost_connection(key).await;
        let stat = GLOBAL_STAT.read().await;
        assert_eq!(stat.connections.get(key), Some(&(1, 1)));
    }

    #[tokio::test]
    async fn test_update_traffic_stats() {
        let key = "test_traffic_unique_3";
        update_traffic_stats(key, 1024, 2048, 1).await;
        let stat = GLOBAL_STAT.read().await;
        assert_eq!(stat.traffic.get(key), Some(&(1024, 2048)));
        assert_eq!(stat.errors.get(key), Some(&1));
    }

    #[tokio::test]
    async fn test_update_metric() {
        let key = "test_metric_unique_4";
        update_metric(key, 42).await;
        let stat = GLOBAL_STAT.read().await;
        assert_eq!(stat.metrics.get(key), Some(&42));
    }

    #[test]
    fn test_show_stats_sync_format() {
        let output = show_stats_sync();
        // New table format should contain headers
        assert!(output.contains("service"));
        assert!(output.contains("in"));
        assert!(output.contains("out"));
    }
}
