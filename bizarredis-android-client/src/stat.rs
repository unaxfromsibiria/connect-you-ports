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

    fn format_stats(&self) -> String {
        let mut output = String::from("traffic:\n");
        for (key, (in_value, out_value)) in &self.traffic {
            let print_value = |value: usize| -> (f64, &'static str) {
                if value >= 1024 * 1024 {
                    let mb = (value as f64 / (1024.0 * 1024.0)) * 10.0;
                    (mb.round() / 10.0, "mb")
                } else {
                    let kb = (value as f64 / 1024.0) * 10.0;
                    (kb.round() / 10.0, "kb")
                }
            };
            let (in_val, in_unit) = print_value(*in_value);
            let (out_val, out_unit) = print_value(*out_value);
            output.push_str(&format!("  target {} in: {:.1} {} out: {:.1} {}\n", key, in_val, in_unit, out_val, out_unit));
        }

        output.push_str("connections:\n");
        for (key, (total, lost)) in &self.connections {
            output.push_str(&format!("  target {} total: {} lost: {}\n", key, total, lost));
        }

        output.push_str("errors:\n");
        for (key, error_count) in &self.errors {
            output.push_str(&format!("  target {} errors: {}\n", key, error_count));
        }

        output.push_str("metrics:\n");
        for (key, value) in &self.metrics {
            output.push_str(&format!("  target {} value: {}\n", key, value));
        }

        output
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
    stat.format_stats()
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
        // We cannot guarantee empty state in parallel tests with global static.
        // Just verify the output contains expected headers.
        let output = show_stats_sync();
        assert!(output.contains("traffic:"));
        assert!(output.contains("connections:"));
        assert!(output.contains("errors:"));
        assert!(output.contains("metrics:"));
    }

    #[tokio::test]
    async fn test_format_stats_units() {
        let key = "test_units_unique_5";
        update_traffic_stats(key, 2_000_000, 500, 0).await;
        let stat = GLOBAL_STAT.read().await;
        // Check if key exists first to debug
        assert!(stat.traffic.contains_key(key), "Key {} not found in traffic", key);
        let output = stat.format_stats();
        // The logic: 2_000_000 bytes -> ~1.9 MB. 
        // Format string: "in: {:.1} mb"
        assert!(output.contains("mb"), "Output should contain 'mb' for large traffic. Output:\n{}", output);
    }
}
