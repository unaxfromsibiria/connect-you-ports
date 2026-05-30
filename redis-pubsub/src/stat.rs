use std::collections::HashMap;
use log::info;

pub struct Stat {
    values: HashMap<String, (usize, usize)>,
    connections: HashMap<String, (usize, usize)>,
    errors: HashMap<String, usize>,
    raw_data_size: u64,
    transfer_data_size: u64,
}

pub trait StatManage {
    fn add_input_traffic(&mut self, ip: &str, target: &str, value: usize);
    fn add_output_traffic(&mut self, ip: &str, target: &str, value: usize);
    fn connection_new(&mut self, ip: &str, target: &str);
    fn connection_lost(&mut self, ip: &str, target: &str);
    fn add_error(&mut self, ip: &str, target: &str);
    fn update_data_size_rate(&mut self, payload_size: usize, raw_size: usize);
    fn join(&mut self, other_stat: &Stat);
    fn clear(&mut self);
    fn show(&self);
    fn is_empty(&self) -> bool;
    fn new() -> Self;
}

impl StatManage for Stat {
    fn new() -> Self {
        Stat {
            values: HashMap::new(),
            connections: HashMap::new(),
            errors: HashMap::new(),
            raw_data_size: 0,
            transfer_data_size: 0,
        }
    }

    fn add_error(&mut self, ip: &str, target: &str) {
        let key = format!("{} {}", target, ip);
        let entry = self.errors.entry(key).or_insert(0);
        *entry += 1;
    }

    fn add_input_traffic(&mut self, ip: &str, target: &str, value: usize) {
        let key = format!("{} {}", target, ip);
        let entry = self.values.entry(key).or_insert((0, 0));
        entry.0 += value;
    }

    fn add_output_traffic(&mut self, ip: &str, target: &str, value: usize) {
        let key = format!("{} {}", target, ip);
        let entry = self.values.entry(key).or_insert((0, 0));
        entry.1 += value;
    }

    fn connection_new(&mut self, ip: &str, target: &str) {
        let key = format!("{} {}", target, ip);
        let entry = self.connections.entry(key).or_insert((0, 0));
        entry.0 += 1;
    }

    fn connection_lost(&mut self, ip: &str, target: &str) {
        let key = format!("{} {}", target, ip);
        let entry = self.connections.entry(key).or_insert((0, 0));
        entry.1 += 1;
    }

    fn update_data_size_rate(&mut self, payload_size: usize, raw_size: usize) {
        self.transfer_data_size += payload_size as u64;
        self.raw_data_size += raw_size as u64;
    }

    fn clear(&mut self) {
        self.values.clear();
        self.connections.clear();
        self.errors.clear();
        self.raw_data_size = 0;
        self.transfer_data_size = 0;
    }

    fn join(&mut self, other_stat: &Stat) {
        for (key, (in_value, out_value)) in &other_stat.values {
            let entry = self.values.entry(key.clone()).or_insert((0, 0));
            entry.0 += in_value;
            entry.1 += out_value;
        }
        for (key, (new_connections, lost_connections)) in &other_stat.connections {
            let entry = self.connections.entry(key.clone()).or_insert((0, 0));
            entry.0 += new_connections;
            entry.1 += lost_connections;
        }
        for (key, error_count) in &other_stat.errors {
            let entry = self.errors.entry(key.clone()).or_insert(0);
            *entry += error_count;
        }
        self.raw_data_size += other_stat.raw_data_size;
        self.transfer_data_size += other_stat.transfer_data_size;
    }

    fn show(&self) {
        info!("traffic:");
        for (key, (in_value, out_value)) in &self.values {
            let print_value = |value: usize| -> (f64, &'static str) {
                if value >= 1024 * 1024 {
                    let mb = (value as f64 / (1024.0 * 1024.0)) * 10.0;
                    (mb.round() / 10.0, "mb")
                } else {
                    let kb = (value as f64 / 1024.0) * 10.0;
                    (kb.round() / 10.0, "kb")
                }
            };

            let (in_value_formatted, in_unit) = print_value(*in_value);
            let (out_value_formatted, out_unit) = print_value(*out_value);

            info!(
                "  target {} in: {:.1} {} out: {:.1} {}",
                key, in_value_formatted, in_unit, out_value_formatted, out_unit
            );
        }

        info!("connections:");
        for (key, (new_connections, lost_connections)) in &self.connections {
            info!(
                "  target {} new: {} lost: {}",
                key, new_connections, lost_connections
            );
        }

        info!("errors:");
        for (key, error_count) in &self.errors {
            info!(
                "  target {} errors: {}",
                key, error_count
            );
        }

        if self.raw_data_size > 0 {
            let rate = (self.transfer_data_size as f64 / self.raw_data_size as f64) * 100.0 - 100.0;
            info!("payload rate: {}{:.1}%", if rate > 0.0 {"+"} else {""}, rate);
        } else {
            info!("payload rate: +0.0%");
        }
    }

    fn is_empty(&self) -> bool {
        self.raw_data_size < 1 && self.transfer_data_size < 1
    }
}
