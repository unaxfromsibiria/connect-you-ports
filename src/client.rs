use crate::common;
use crate::stat;
use crate::data;

use tokio::net::TcpListener;
use tokio::net::UdpSocket;

use common::{Settings, code_name, fast_name};
use data::{client_data_topic, server_data_topic, DataHandlerSettings, DataHandler};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use futures::StreamExt;
use std::collections::HashMap;
use std::time::Duration;
use redis::AsyncCommands;
use tokio::task::JoinSet;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::sleep;
use log::{info, warn, error, debug};
use stat::{Stat, StatManage};

pub async fn client_processing(settings: Settings, data_handler: DataHandlerSettings, redis_c: redis::Client) -> JoinSet<()> {
    let mut set = JoinSet::new();
    let buffer_size: usize = settings.buffer_size as usize;
    let all_stat = Stat::new();
    let stat_delay = settings.stat_delay as u64;
    let arc_stat = Arc::new(RwLock::new(all_stat));
    let arc_show_stat = arc_stat.clone();
    let tcp_keys: Vec<_> = settings.tcp_sockets.keys().cloned().collect();
    for serv_name in settings.udp_sockets.keys() {
        if tcp_keys.contains(serv_name) {
            error!(
                "Configuration has duplicate service names: '{}' in UDP and TCP sockets",
                serv_name
            );
            return set;
        }
    }

    set.spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(stat_delay)).await;
            let stat_guard = arc_show_stat.read().await;
            stat_guard.show();
        }
    });
    // TCP
    for (service_name, ip_port_map) in settings.tcp_sockets.iter() {
        let Some((with_ip, with_port)) = ip_port_map.iter().next() else {
            error!("No IP-Port mapping found for server {}", service_name);
            continue;
        };
        let service_code = code_name(service_name);
        let client_arc_stat = arc_stat.clone();
        let ip = with_ip.clone();
        let port = with_port.clone();
        let service_name = service_name.clone();
        let service_data_handler = data_handler.clone();
        let r_con = redis_c.get_multiplexed_async_connection().await.unwrap();
        let redis_client = redis_c.clone();

        set.spawn(async move {
            let listener = match TcpListener::bind((ip, port)).await {
                Ok(listener) => listener,
                Err(err) => {
                    error!("Failed to bind to {}:{} : {}", ip, port, err);
                    return;
                }
            };
            info!("Listening on tcp://{}:{} service {} ({})", ip, port, service_name, service_code);
            let serv_name = service_name.clone();
            loop {
                let (stream, c_addr) = match listener.accept().await {
                    Ok(result) => result,
                    Err(err) => {
                        error!("Failed to accept connection: {}", err);
                        let mut stat = client_arc_stat.write().await;
                        stat.add_error(&ip.to_string(), &serv_name);
                        continue;
                    }
                };
                let c_ip = c_addr.ip().to_string();
                let serv_code = service_code.clone();
                let delay = settings.new_connection_delay as u64;
                let idle_limit = settings.idle_tcp_limit as u64;
                let conn_arc_stat = client_arc_stat.clone();
                let serv_name = serv_name.clone();
                let data_handler = service_data_handler.clone();
                let mut r_con = r_con.clone();
                let mut pubsub_conn = redis_client.get_async_pubsub().await.unwrap();

                tokio::spawn(async move {
                    let (mut reader, mut writer) = tokio::io::split(stream);
                    let mut buf = vec![0; buffer_size];
                    let c_id = fast_name();
                    let client_topic = client_data_topic(&serv_code, &c_id);
                    let mut client_stat = Stat::new();
                    info!("New client {} - {} in {}", c_ip, c_id, serv_code);
                    // new client declaration
                    let (payload, topic) = data_handler.make_hello_topic_message(&serv_code, &c_id, true);
                    match r_con.publish::<&str, &Vec<u8>, ()>(&topic, &payload).await {
                        Ok(_) => {
                            debug!("New client '{}' request for service '{}'", c_id, serv_code);
                            client_stat.connection_new(&c_ip.to_string(), &serv_name);
                            sleep(Duration::from_millis(delay)).await;
                        },
                        Err(err) => {
                            error!("Unable to send hello {} - {} in {}: {}", c_ip, c_id, serv_code, err);
                            client_stat.add_error(&c_ip.to_string(), &serv_name);
                            client_stat.connection_lost(&c_ip.to_string(), &serv_name);
                            return;
                        }
                    }
                    match pubsub_conn.subscribe(client_topic).await {
                        Ok(_) => {
                            debug!("Subscribed to client {} service {}", c_id, serv_code);
                        },
                        Err(err) => {
                            error!("Failed to subscribe to Redis topic {}: {}", c_id, err);
                            client_stat.add_error(&c_ip.to_string(), &serv_name);
                            client_stat.connection_lost(&c_ip.to_string(), &serv_name);
                            return;
                        }
                    }
                    let mut pubsub_stream = pubsub_conn.on_message();
                    let mut get_count = 0;
                    // new connection processing
                    loop {
                        tokio::select! {
                            // read client connection
                            n = reader.read(&mut buf) => {
                                let n = match n {
                                    Ok(n) => n,
                                    Err(err) => {
                                        error!("Failed to read: {}", err);
                                        break;
                                    }
                                };
                                if n == 0 {
                                    debug!("Closing read TCP connection for {}", c_id);
                                    if get_count > 0 {
                                        let (payload, topic) = data_handler.make_quit_message(&serv_code, &c_id, false);
                                        match r_con.publish::<&str, &Vec<u8>, ()>(&topic, &payload).await {
                                            Ok(_) => {
                                                info!("Closing read TCP connection for {} with server notification", c_id);
                                            },
                                            Err(err) => {
                                                warn!("Sending quit {} - {} in {}: {}", c_ip, c_id, serv_code, err);
                                            }
                                        }

                                    }
                                    break;
                                }
                                let (payload, topic) = data_handler.make_data_message(&buf[..n], &serv_code, &c_id, false);
                                match r_con.publish::<&str, &Vec<u8>, ()>(&topic, &payload).await {
                                    Ok(_) => {
                                        debug!("Sending {} bytes for {} to topic '{}'", payload.len(), c_id, topic);
                                        client_stat.add_input_traffic(&c_ip, &serv_name, n);
                                        client_stat.update_data_size_rate(payload.len(), n);
                                    },
                                    Err(err) => {
                                        error!("{} - {} in {}: {}", c_ip, c_id, serv_code, err);
                                        client_stat.add_error(&c_ip.to_string(), &serv_name);
                                        break;
                                    }
                                }
                            }
                            // read from redis and write to client
                            msg_res = pubsub_stream.next() => {
                                match msg_res {
                                    Some(msg) => {
                                        get_count += 1;
                                        let payload: Vec<u8> = msg.get_payload().unwrap();
                                        let new_msg = data_handler.load_data_message(&payload);
                                        if new_msg.e.is_empty() {
                                            if new_msg.x {
                                                warn!("Closing by request {} - {} in {}", c_ip, c_id, serv_code);
                                                break;
                                            } else {
                                                if writer.write_all(&new_msg.d).await.is_err() {
                                                    error!("Failed to write to TCP connection {} - {} in {}", c_ip, c_id, serv_code);
                                                    client_stat.add_error(&c_ip.to_string(), &serv_name);
                                                    let (payload, topic) = data_handler.make_quit_message(&serv_code, &c_id, false);
                                                    match r_con.publish::<&str, &Vec<u8>, ()>(&topic, &payload).await {
                                                        Ok(_) => {},
                                                        Err(err) => {
                                                            warn!("Sending quit {} - {} in {}: {}", c_ip, c_id, serv_code, err);
                                                        }
                                                    }
                                                    // easy to break connection without retry policy
                                                    break;
                                                } else {
                                                    client_stat.add_output_traffic(&c_ip.to_string(), &serv_name, new_msg.d.len());
                                                    client_stat.update_data_size_rate(payload.len(), new_msg.d.len());
                                                    debug!("Sending {} bytes to client {} {}", new_msg.d.len(), c_id, c_ip);
                                                }
                                            }
                                        } else {
                                            warn!("Wrong data format {} - {} in {}: {}", c_ip, c_id, serv_code, new_msg.e);
                                        }
                                    }
                                    None => {
                                        error!("Redis subscription stream ended");
                                        break;
                                    }
                                }
                            }
                            // idle connection
                            _ = sleep(Duration::from_secs(idle_limit)) => {
                                let (payload, topic) = data_handler.make_quit_message(&serv_code, &c_id, false);
                                match r_con.publish::<&str, &Vec<u8>, ()>(&topic, &payload).await {
                                    Ok(_) => {
                                        info!("Sending quit after idle connection {} {}", c_id, serv_code);
                                    },
                                    Err(err) => {
                                        warn!("Sending quit {} - {} in {} error: {}", c_ip, c_id, serv_code, err);
                                    }
                                }
                                break;
                            }
                        }
                    }
                    // end connection processing
                    if !(client_stat.is_empty()) {
                        info!("Data transfer with {} has been completed", c_ip);
                        let mut stat = conn_arc_stat.write().await;
                        stat.join(&client_stat);
                        stat.connection_lost(&c_ip.to_string(), &serv_name);
                    }
                });
            }
        });
    }
    // UDP
    for (service_name, ip_port_map) in settings.udp_sockets.iter() {
        let Some((with_ip, with_port)) = ip_port_map.iter().next() else {
            error!("No IP-Port mapping found for server {}", service_name);
            continue;
        };
        let service_code = code_name(service_name);
        let client_arc_stat = arc_stat.clone();
        let ip = with_ip.clone();
        let port = with_port.clone();
        let service_name = service_name.clone();
        let service_data_handler = data_handler.clone();
        let mut r_con = redis_c.get_multiplexed_async_connection().await.unwrap();
        let redis_client = redis_c.clone();
        let client_name = settings.client_name.clone();

        set.spawn(async move {
            let server_socket = match UdpSocket::bind((ip, port)).await {
                Ok(result) => {
                    info!("Listening on udp://{}:{} service {} ({})", ip, port, service_name, service_code);
                    result
                },
                Err(err) => {
                    error!("Failed to accept connection: {}", err);
                    let mut stat = client_arc_stat.write().await;
                    stat.add_error(&ip.to_string(), &service_name);
                    return;
                }
            };

            let mut buf = vec![0; buffer_size];
            let out_topic = client_data_topic(&service_code, "cc-ud");
            let in_topic = server_data_topic(&service_code, "sc-ud");
            let idle_limit = settings.idle_udp_limit as u64;
            let mut current_clients = HashMap::new();
            let mut client_stat = Stat::new();
            let mut pubsub_conn = redis_client.get_async_pubsub().await.unwrap();
            match pubsub_conn.subscribe(in_topic).await {
                Ok(_) => {
                    debug!("Subscribed to UDP service {} for {}", service_name, client_name);
                },
                Err(err) => {
                    error!("Failed to subscribe to Redis topic {}: {}", service_code, err);
                    return;
                }
            }
            let mut pubsub_stream = pubsub_conn.on_message();
            let mut c_ip = "".to_string();
            let mut current_peer_code = "".to_string();

            loop {
                tokio::select! {
                    // read from client
                    res = server_socket.recv_from(&mut buf) => {
                        let (n, peer) = match res {
                            Ok((n, peer)) => {(n, peer)},
                            Err(err) => {
                                error!("Failed to read {}: {}", service_name, err);
                                break;
                            }
                        };
                        if n == 0 {
                            debug!("Closing read UDP {}", peer);
                            break;
                        }
                        let peer_code = code_name(&format!("{}-{}", client_name, peer.to_string()));
                        current_peer_code = peer_code.clone();
                        if !current_clients.contains_key(&peer_code) {
                            info!("New UDP client {} id {}", peer, peer_code);
                            client_stat.connection_new(&peer.ip().to_string(), &service_name);
                        }
                        let out_peer = current_clients.entry(peer_code.to_string()).or_insert(peer);
                        c_ip = out_peer.ip().to_string();
                        let (payload, _) = service_data_handler.make_data_message(&buf[..n], &service_code, &peer_code, false);
                        match r_con.publish::<&str, &Vec<u8>, ()>(&out_topic, &payload).await {
                            Ok(_) => {
                                debug!("Sending {} bytes for {} to topic '{}'", payload.len(), peer_code, out_topic);
                                client_stat.add_input_traffic(&c_ip, &service_name, n);
                                client_stat.update_data_size_rate(payload.len(), n);
                            },
                            Err(err) => {
                                error!("{} - {} in {}: {}", c_ip, peer_code, service_name, err);
                                client_stat.add_error(&c_ip.to_string(), &service_name);
                                break;
                            }
                        }
                    }
                    // write to peer
                    msg_res = pubsub_stream.next() => {
                        match msg_res {
                            Some(msg) => {
                                let payload: Vec<u8> = msg.get_payload().unwrap();
                                let new_msg = service_data_handler.load_data_message(&payload);
                                client_stat.update_data_size_rate(payload.len(), new_msg.d.len());
                                if new_msg.e.is_empty() {
                                    if new_msg.x {
                                        warn!("Server sent quit for {}: {}", new_msg.c, new_msg.e);
                                        continue;
                                    } else {
                                        let client_peer = current_clients.iter().find(|&(code, _)| code.eq(&new_msg.c)).map(|(_, p)| p);
                                        if client_peer.is_none() {
                                            error!("Client peer not found for code: {}", new_msg.c);
                                            continue;
                                        }
                                        let ip = client_peer.unwrap();
                                        match server_socket.send_to(&new_msg.d, ip).await {
                                            Ok(sent_n) => {
                                                client_stat.add_output_traffic(&ip.ip().to_string(), &service_name, sent_n);
                                            },
                                            Err(err) => {
                                                error!("Client send error {} to {}: {}", new_msg.c, ip, err);
                                                break;
                                            }
                                        }
                                    }
                                } else {
                                    error!("Server sent error for {}: {}", new_msg.c, new_msg.e);
                                }
                            },
                            None => {
                                error!("{} in {}: no messages in topic", c_ip, service_name);
                                client_stat.add_error(&c_ip, &service_name);
                                break;
                            }
                        }
                    }
                    // timeout operation
                    _ = sleep(Duration::from_secs(idle_limit)) => {
                        warn!("No UDP transfer {} in {}", c_ip, service_code);
                        if !current_peer_code.is_empty() {
                            let (payload, _) = service_data_handler.make_quit_message(&service_code, &current_peer_code, false);
                            match r_con.publish::<&str, &Vec<u8>, ()>(&out_topic, &payload).await {
                                Ok(_) => {
                                    info!("Sending quit {} bytes for {} to topic '{}'", payload.len(), current_peer_code, out_topic);
                                },
                                Err(err) => {
                                    error!("{} - {} in {}: {}", c_ip, current_peer_code, service_name, err);
                                    client_stat.add_error(&c_ip.to_string(), &service_name);
                                    break;
                                }
                            }
                        }
                        if !(client_stat.is_empty()) {
                            let mut stat = client_arc_stat.write().await;
                            stat.join(&client_stat);
                            client_stat.clear();
                        }
                    }
                }
                if !(client_stat.is_empty()) {
                    let mut stat = client_arc_stat.write().await;
                    stat.join(&client_stat);
                    stat.connection_lost(&c_ip, &service_name);
                }
            }
            info!("UDP data transfer with {} has been completed", c_ip);
        });
    }
    set
}
