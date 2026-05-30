# Rust-based TCP Tunnel: Client-Server Application for Forwarding TCP and UDP Traffic

This project implements a high-performance client-server application that establishes a secure **TCP tunnel** between a local client and a remote server. Once the TCP connection is established, the tunnel can forward multiple local TCP connections and UDP traffic streams to remote destinations.

## The project can be used as a tunnel into the cloud infrastructure for development

![Use case](img/use-case.png)

You can forward multiple TCP and UDP connections through the persistent TCP tunnel, launch several infrastructure services without direct access, and define named enumerations for each service: `TCP_SOCKETS='db:127.0.0.1:5432;dev-api:127.0.0.1:8080;rabbit:127.0.0.1:5672'` `UDP_SOCKETS='iperf-udp:0.0.0.0:9092;dns:0.0.0.0:5553'`

On the client side, all these sockets are accessible locally. On the server side, connections are established to the appropriate services based on the target configuration, e.g.: `SERVER_TCP_TARGET='db:host-in-cloud-2:5432;dev-api:host-in-cloud-3:8080;rabbit:host-in-cloud-4:5672'` and for the UDP sockets: `SERVER_UDP_TARGET='iperf-udp:0.0.0.0:9092;dns:8.8.8.8:53'`

## Configuration Features

To simplify configuration for different loads, various parameters are now grouped and set via a single environment variable `LOADING_LEVEL`.
Possible values for the variable:

- `LOADING_LEVEL=default` - or empty value, a suitable configuration for many applications
- `LOADING_LEVEL=high` - for high traffic usage or multiple clients
- `LOADING_LEVEL=extremely` - if data exchange is very intensive
- `LOADING_LEVEL=low` - if the client or server is running on very limited resources

Additionally, you may need to configure the following to optimize for your specific services:

- `WORKERS=8` - should be increased if there are many targets or clients
- `READ_BUFFER_SIZE=16384` - read buffer size; sometimes it needs to be specified manually (a manually set value overrides the one determined by `LOADING_LEVEL`)
- `RUST_LOG=warning` - set this if you don't need extensive logging

Some services work better with a buffer larger than 8kb, while others prefer 2kb. It is recommended to experiment with this parameter.

Other configurations are available as examples below.

## Example using Docker

To set up the server side:

```bash
make example_server -s
# edit compose file to set env variables
docker compose up -d --build
```

and client side almost the same way:

```bash
make example_client -s
# edit compose file to set env variables
docker compose up -d --build
```
