use crate::common;
use crate::stat;
use crate::data;

use common::{Settings, code_name, TOPIC_NAME_NEW_CLIENT};
use data::{server_data_topic, client_data_topic, DataHandlerSettings, DataHandler, DataMsg};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use futures::StreamExt;
use redis::AsyncCommands;
use tokio::task::JoinSet;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;
use log::{info, warn, error, debug};
use stat::{Stat, StatManage};
use std::time::Duration;
use tokio::time::sleep;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use std::collections::HashMap;

async fn handle_udp_transfer(
    socket: Arc<tokio::net::UdpSocket>,
    service_code: String,
    service_name: String,
    target_ip: IpAddr,
    target_port: u16,
    client_id: String,
    tx: tokio::sync::mpsc::Sender<Vec<u8>>,
    mut rx: tokio::sync::mpsc::Receiver<DataMsg>,
    data_handler: DataHandlerSettings,
    stat: Arc<RwLock<Stat>>,
    idle_limit: u64,
    buffer_size: usize,
) {
    let mut read_buffer = vec![0u8; buffer_size];
    let mut local_stat = Stat::new();
    local_stat.connection_new(&target_ip.to_string(), &service_name);
    info!("New client {} transferring data with {} ({}) via UDP to udp://{}:{}", client_id, service_name, service_code, target_ip, target_port);
    loop {
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Some(msg) => {
                        if msg.x {
                            warn!("Client {} requested closing for UDP service {} ({})", client_id, service_name, service_code);
                            break;
                        }
                        match socket.send_to(&msg.d, (target_ip, target_port)).await {
                            Ok(n) => {
                                local_stat.add_output_traffic(&target_ip.to_string(), &service_name, n);
                            },
                            Err(err) => {
                                error!("Failed to send UDP data to {} ({}) from {}: {}", target_ip, service_name, client_id, err);
                                local_stat.add_error(&target_ip.to_string(), &service_name);
                                break;
                            }
                        }
                    },
                    None => {
                        break;
                    }
                }
            }
            res = socket.recv_from(&mut read_buffer) => {
                match res {
                    Ok((n, addr)) => {
                        if n == 0 {
                            warn!("No data received from {} for {} in service {}", addr, service_name, client_id);
                            break;
                        }
                        let (data, _) = data_handler.make_data_message(&read_buffer[..n], &service_code, &client_id, true);
                        let m = data.len();
                        local_stat.update_data_size_rate(m, n);
                        if tx.send(data).await.is_err() {
                            error!("Failed to send data to output channel for {} ({}) from {}", service_name, client_id, addr);
                        } else {
                            local_stat.add_input_traffic(&target_ip.to_string(), &service_code, m);
                        }
                    }
                    Err(err) => {
                        error!("Error receiving UDP data for {} ({}): {}", service_name, client_id, err);
                        local_stat.add_error(&target_ip.to_string(), &service_name);
                        break;
                    }
                }
            }
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(idle_limit)) => {
                warn!("Idle timeout reached for UDP data transfer with {} ({}) for {}", target_ip, service_name, client_id);
                let (data, _) = data_handler.make_quit_message(&service_code, &client_id, true);
                if tx.send(data).await.is_err() {
                    error!("Failed to send quit message for {} ({})", service_name, client_id);
                } else {
                    info!("Sent quit message for {} ({})", service_name, client_id);
                }
                break;
            }
        }
    }
    if !local_stat.is_empty() {
        let mut all_stat = stat.write().await;
        all_stat.connection_lost(&target_ip.to_string(), &service_name);
        all_stat.join(&local_stat);
    }
    info!("Closed UDP data transfer with client {} for {} ({}) at udp://{}:{}", client_id, service_name, service_code, target_ip, target_port);
}


pub async fn server_processing(settings: Settings, data_handler: DataHandlerSettings, redis_c: redis::Client) -> JoinSet<()> {
    let stat = Stat::new();
    let mut set = JoinSet::new();
    let tcp_keys: Vec<_> = settings.tcp_targets.keys().cloned().collect();
    for serv_name in settings.udp_targets.keys() {
        if tcp_keys.contains(serv_name) {
            error!("Configuration has duplicate service names: '{}'", serv_name);
            return set;
        }
    }
    let mut targets = Vec::new();
    for (serv_name, addr_map) in settings.tcp_targets.iter() {
        let Some((ip, port)) = addr_map.iter().next() else {
            error!("No target socket found for server {}", serv_name);
            continue;
        };
        let serv_code = code_name(serv_name);
        // just to info
        let (_, topic) = data_handler.make_quit_message(&serv_code, "<client>", false);
        info!("{} ({}) -> tcp://{}:{}", topic, serv_name, ip, port);
        targets.push((true, serv_name.clone(), serv_code.clone(), ip.clone(), *port));
    }
    for (serv_name, addr_map) in settings.udp_targets.iter() {
        let Some((ip, port)) = addr_map.iter().next() else {
            error!("No target socket found for server {}", serv_name);
            continue;
        };
        let serv_code = code_name(serv_name);
        let udp_in_topic = client_data_topic(&serv_code, "cc-ud");
        info!("{} ({}) -> udp://{}:{}", udp_in_topic, serv_name, ip, port);
        targets.push((false, serv_name.clone(), serv_code.clone(), ip.clone(), *port));
    }
    let mut pubsub_conn = redis_c.get_async_pubsub().await.unwrap();
    let buffer_size: usize = settings.buffer_size as usize;
    let targets_list = targets.clone();
    let arc_stat = Arc::new(RwLock::new(stat));
    let client_arc_stat = arc_stat.clone();
    let show_arc_stat = arc_stat.clone();
    let stat_delay = settings.stat_delay as u64;

    set.spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(stat_delay)).await;
            let stat_guard = show_arc_stat.read().await;
            stat_guard.show();
        }
    });
    let r_con = redis_c.get_multiplexed_async_connection().await.unwrap();
    let data_h = data_handler.clone();
    let redis_client = redis_c.clone();
    // TCP processing
    set.spawn(async move {
        let new_client_topic = TOPIC_NAME_NEW_CLIENT.to_string();
        match pubsub_conn.subscribe(new_client_topic).await {
            Ok(_) => {
                info!("New clients waiting in '{}'", TOPIC_NAME_NEW_CLIENT);
            },
            Err(err) => {
                error!("Failed to subscribe to redis topic {}: {}", TOPIC_NAME_NEW_CLIENT, err);
                return;
            }
        }
        let mut pubsub_stream = pubsub_conn.on_message();
        while let Some(msg) = pubsub_stream.next().await {
            let payload: Vec<u8> = msg.get_payload().unwrap();
            let c_msg = data_h.load_hello_message(&payload);
            if c_msg.c.is_empty() {
                warn!("Incorrect protocol for new client: {}", c_msg.s);
            } else {
                info!("New client '{}' id: {}", c_msg.s, c_msg.c);
                let mut target = None;
                for (is_tcp, s_name, service_code, ip, port) in targets_list.iter() {
                    if (*is_tcp && c_msg.p == 1) && c_msg.s == *service_code {
                        target = Some((s_name.clone(), ip.clone(), port.clone(), service_code.clone()));
                        break;
                    }
                }

                if let Some((service_name, tcp_ip, tcp_port, service_code)) = target {
                    let tcp_stream = match tokio::net::TcpStream::connect((tcp_ip, tcp_port)).await {
                        Ok(stream) => {
                            let mut stat = client_arc_stat.write().await;
                            stat.connection_new(&tcp_ip.to_string(), &service_name);
                            stream
                        },
                        Err(err) => {
                            error!("Failed to connect to TCP target '{}' {}:{} : {}", service_name, tcp_ip, tcp_port, err);
                            continue;
                        }
                    };

                    let client_id = c_msg.c;
                    info!("New connection '{}' ({}) for {} -> {}:{}", service_code, service_name, client_id, tcp_ip, tcp_port);
                    let data_topic = server_data_topic(&service_code, &client_id);
                    let mut pubsub_in = redis_client.get_async_pubsub().await.unwrap();
                    let conn_arc_stat = client_arc_stat.clone();
                    let data_h = data_h.clone();
                    let mut r_con = r_con.clone();
                    let idle_limit = settings.idle_tcp_limit as u64;

                    tokio::spawn(async move {
                        let (mut reader, mut writer) = tokio::io::split(tcp_stream);
                        let mut read_buffer = vec![0u8; buffer_size];

                        match pubsub_in.subscribe(data_topic).await {
                            Ok(_) => {},
                            Err(err) => {
                                error!("Failed to subscribe to Redis topic {}: {}", client_id, err);
                                return;
                            }
                        }
                        let mut pubsub_stream = pubsub_in.on_message();
                        let mut local_stat = Stat::new();

                        loop {
                            tokio::select! {
                                read_result = reader.read(&mut read_buffer) => {
                                    match read_result {
                                        Ok(0) => {
                                            info!("Connection closed by peer");
                                            let (payload, topic) = data_h.make_quit_message(&service_code, &client_id, true);
                                            match r_con.publish::<&str, &Vec<u8>, ()>(&topic, &payload).await {
                                                Ok(_) => {
                                                    debug!("Sending quit to '{}' {} bytes", topic, payload.len());
                                                },
                                                Err(err) => {
                                                    error!("Sending data error to {} from service {}: {}", client_id, service_code, err);
                                                    break;
                                                }
                                            }
                                            break;
                                        },
                                        Ok(n) => {
                                            let data = &read_buffer[..n];
                                            let (payload, topic) = data_h.make_data_message(&data, &service_code, &client_id, true);
                                            match r_con.publish::<&str, &Vec<u8>, ()>(&topic, &payload).await {
                                                Ok(_) => {
                                                    debug!("Sending to '{}' {} bytes", topic, payload.len());
                                                },
                                                Err(err) => {
                                                    error!("Sending data error to {} from service {}: {}", client_id, service_code, err);
                                                    local_stat.add_error(&tcp_ip.to_string(), &service_name);
                                                    break;
                                                }
                                            }
                                            local_stat.add_input_traffic(&tcp_ip.to_string(), &service_name, n);
                                            local_stat.update_data_size_rate(payload.len(), n);
                                        },
                                        Err(err) => {
                                            error!("Failed to read from TCP stream for {} {}: {}", client_id, service_code, err);
                                            local_stat.add_error(&tcp_ip.to_string(), &service_name);
                                            break;
                                        }
                                    }
                                },
                                msg = pubsub_stream.next() => {
                                    match msg {
                                        Some(msg) => {
                                            let payload: Vec<u8> = msg.get_payload().unwrap();
                                            let new_msg = data_h.load_data_message(&payload);
                                            if new_msg.e.is_empty() {
                                                if new_msg.x {
                                                    warn!("Closing by request from {} service {}", client_id, service_code);
                                                    break;
                                                } else {
                                                    if let Err(err) = writer.write_all(&new_msg.d).await {
                                                        error!("Failed to write to TCP stream: {} for {}", service_code, err);
                                                        let (payload, topic) = data_h.make_quit_message(&service_code, &client_id, true);
                                                        match r_con.publish::<&str, &Vec<u8>, ()>(&topic, &payload).await {
                                                            Ok(_) => {
                                                            },
                                                            Err(err) => {
                                                                error!("Sending quit error to {} from service {}: {}", client_id, service_code, err);
                                                                local_stat.add_error(&tcp_ip.to_string(), &service_name);
                                                                break;
                                                            }
                                                        }
                                                        break;
                                                    } else {
                                                        local_stat.add_output_traffic(&tcp_ip.to_string(), &service_name, new_msg.d.len());
                                                        local_stat.update_data_size_rate(payload.len(), new_msg.d.len());
                                                        debug!("Sending to '{}' from {} {} bytes", service_name, client_id, new_msg.d.len());
                                                    }
                                                }
                                            } else {
                                                warn!("From client topic '{}' wrong message {}", client_id, new_msg.e);
                                            }
                                        },
                                        None => {
                                            info!("PubSub stream ended for {}", client_id);
                                            break;
                                        }
                                    }
                                }
                                // idle connection
                                _ = sleep(Duration::from_secs(idle_limit)) => {
                                    let (payload, topic) = data_h.make_quit_message(&service_code, &client_id, true);
                                    match r_con.publish::<&str, &Vec<u8>, ()>(&topic, &payload).await {
                                        Ok(_) => {
                                            info!("Sending quit after idle connection {} {}", client_id, service_code);
                                        },
                                        Err(err) => {
                                            warn!("Sending quit {} in {} error: {}", client_id, service_code, err);
                                        }
                                    }
                                    break;
                                }
                            }
                        }
                        if !local_stat.is_empty() {
                            let mut stat = conn_arc_stat.write().await;
                            stat.connection_lost(&tcp_ip.to_string(), &service_name);
                            stat.join(&local_stat);
                        }
                    });
                }
            }
        }
    });
    // UDP processing
    let m_r_con = redis_c.get_multiplexed_async_connection().await.unwrap();
    for (is_tcp, serv_name, serv_code, ip, port) in targets.iter() {
        if *is_tcp {
            continue;
        }
        let service_code = serv_code.to_string();
        let service_name = serv_name.to_string();
        let target_ip = ip.clone();
        let target_port = port.clone();
        let data_h = data_handler.clone();
        let redis_connection = redis_c.clone();
        let client_arc_stat = arc_stat.clone();
        let mut r_con = m_r_con.clone();
        let idle_limit = settings.idle_udp_limit;
        let buf_sizs = settings.buffer_size;
        let udp_bind_from = match SocketAddr::from_str(&settings.udp_bind_from) {
            Ok(addr_new) => addr_new,
            Err(err) => {
                error!("Incorrect address in settings {}: {}", settings.udp_bind_from, err);
                break;
            }
        };

        set.spawn(async move {
            let udp_in_topic = client_data_topic(&service_code, "cc-ud");
            let udp_out_topic = server_data_topic(&service_code, "sc-ud");
            let mut pubsub_in = redis_connection.get_async_pubsub().await.unwrap();
            let mut clients: HashMap<String, mpsc::Sender<DataMsg>> = HashMap::new();
            let (client_tx, mut client_rx) = mpsc::channel(512);

            match pubsub_in.subscribe(udp_in_topic.clone()).await {
                Ok(_) => {},
                Err(err) => {
                    error!("Failed to subscribe to Redis topic {}: {}", udp_in_topic, err);
                    return;
                }
            }
            let mut pubsub_stream = pubsub_in.on_message();
            loop {
                tokio::select! {
                    msg = pubsub_stream.next() => {
                        match msg {
                            Some(msg) => {
                                let payload: Vec<u8> = msg.get_payload().unwrap();
                                let new_msg = data_h.load_data_message(&payload);
                                let mut stat = client_arc_stat.write().await;
                                stat.update_data_size_rate(payload.len(), new_msg.d.len());
                                let to_close = if new_msg.x {true} else {false};

                                let client_id = new_msg.c.clone();
                                if new_msg.e.is_empty() {
                                    if let Some(tx) = clients.get(&client_id) {
                                        if tx.send(new_msg).await.is_err() {
                                            clients.remove(&client_id);
                                        }
                                    } else {
                                        let (tx, rx) = mpsc::channel(100);
                                        let c_id = client_id.clone();
                                        clients.insert(client_id.clone(), tx.clone());
                                        let service_code = service_code.to_string();
                                        let service_name = service_name.to_string();
                                        let target_ip = target_ip.clone();
                                        let target_port = target_port.clone();
                                        let arc_stat = client_arc_stat.clone();
                                        let client_tx = client_tx.clone();
                                        let data_h = data_h.clone();
                                        let idle_limit = idle_limit as u64;
                                        let buf_size = buf_sizs.clone();
                                        let socket = match UdpSocket::bind(&udp_bind_from).await {
                                            Ok(socket) => socket,
                                            Err(err) => {
                                                error!("Error binding udp {} for {} service {}: {}", udp_bind_from, client_id, service_code, err);
                                                break;
                                            }
                                        };
                                        let udp = Arc::new(socket);

                                        tokio::spawn(async move {
                                            handle_udp_transfer(
                                                udp,
                                                service_code,
                                                service_name,
                                                target_ip,
                                                target_port,
                                                c_id,
                                                client_tx,
                                                rx,
                                                data_h,
                                                arc_stat,
                                                idle_limit,
                                                buf_size,
                                            ).await;
                                        });
                                        let _ = tx.send(new_msg).await;
                                    }
                                    if to_close {
                                        warn!("Closing by request from {} service {}", client_id, service_code);
                                        clients.remove(&client_id);
                                    }
                                }
                            },
                            None => {
                                error!("Stop waiting for messages from topic for service {}", service_name);
                                break;
                            }
                        }
                    },
                    msg = client_rx.recv() => {
                        match msg {
                            Some(payload) => {
                                match r_con.publish::<&str, &Vec<u8>, ()>(&udp_out_topic, &payload).await {
                                    Ok(_) => {
                                        debug!("Sending {} bytes to {}", payload.len(), udp_out_topic);
                                    },
                                    Err(err) => {
                                        error!("Sending quit error to {} from service {}: {}", udp_out_topic, service_code, err);
                                        break;
                                    }
                                }
                            },
                            None => {
                                continue
                            }
                        }
                    }
                }
            }
        });
    }
    set
}
