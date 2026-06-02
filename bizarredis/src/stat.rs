use tokio::sync::RwLock;
use std::collections::HashMap;
use log::info;
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

    fn show(&self) {
        info!("traffic:");
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
            info!("  target {} in: {:.1} {} out: {:.1} {}", key, in_val, in_unit, out_val, out_unit);
        }
        info!("connections:");
        for (key, (total, lost)) in &self.connections {
            info!("  target {} total: {} lost: {}", key, total, lost);
        }
        info!("errors:");
        for (key, error_count) in &self.errors {
            info!("  target {} errors: {}", key, error_count);
        }
        info!("metrics:");
        for (key, value) in &self.metrics {
            info!("  target {} value: {}", key, value);
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

pub async fn show_stats() {
    let stat = GLOBAL_STAT.read().await;
    stat.show();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_add_connection_increases_current() {
        let service = "test_service_1";
        let stat = GLOBAL_STAT.write().await;
        let initial_current = stat.connections.get(service).map(|(c, _)| *c).unwrap_or(0);
        drop(stat);

        add_connection(service).await;

        let stat = GLOBAL_STAT.read().await;
        let (current, _) = stat.connections.get(service).cloned().unwrap_or((0, 0));
        
        assert_eq!(current, initial_current + 1);
    }

    #[tokio::test]
    async fn test_lost_connection_increases_lost() {
        let service = "test_service_2";
        add_connection(service).await;
        let stat = GLOBAL_STAT.write().await;
        let initial_lost = stat.connections.get(service).map(|(_, l)| *l).unwrap_or(0);
        drop(stat);

        lost_connection(service).await;

        let stat = GLOBAL_STAT.read().await;
        let (_, lost) = stat.connections.get(service).cloned().unwrap_or((0, 0));

        assert_eq!(lost, initial_lost + 1);
    }

    #[tokio::test]
    async fn test_update_traffic_stats() {
        let service = "test_service_3";
        let stat = GLOBAL_STAT.write().await;
        let initial_traffic = stat.traffic.get(service).cloned().unwrap_or((0, 0));
        let initial_errors = stat.errors.get(service).cloned().unwrap_or(0);
        drop(stat);

        update_traffic_stats(service, 100, 200, 2).await;

        let stat = GLOBAL_STAT.read().await;
        let (in_bytes, out_bytes) = stat.traffic.get(service).cloned().unwrap_or((0, 0));
        let errors = stat.errors.get(service).cloned().unwrap_or(0);

        assert_eq!(in_bytes, initial_traffic.0 + 100);
        assert_eq!(out_bytes, initial_traffic.1 + 200);
        assert_eq!(errors, initial_errors + 2);
    }

    #[tokio::test]
    async fn test_update_traffic_stats_no_errors() {
        let service = "test_service_4";

        let stat = GLOBAL_STAT.write().await;
        let initial_errors = stat.errors.get(service).cloned().unwrap_or(0);
        drop(stat);

        update_traffic_stats(service, 50, 100, 0).await; // 0 ошибок

        let stat = GLOBAL_STAT.read().await;
        let errors = stat.errors.get(service).cloned().unwrap_or(0);
        assert_eq!(errors, initial_errors);
    }

    #[tokio::test]
    async fn test_update_metric() {
        let service = "test_service_5";
        let value1 = 42;
        let value2 = 100;

        update_metric(service, value1).await;
        
        let stat = GLOBAL_STAT.read().await;
        assert_eq!(*stat.metrics.get(service).unwrap(), value1);
        drop(stat);

        update_metric(service, value2).await;

        let stat = GLOBAL_STAT.read().await;
        assert_eq!(*stat.metrics.get(service).unwrap(), value2);
    }

    #[tokio::test]
    async fn test_show_stats_does_not_panic() {
        let service = "test_service_6";
        add_connection(service).await;
        update_traffic_stats(service, 100, 200, 1).await;
        update_metric(service, 10).await;
        show_stats().await;
    }
}
