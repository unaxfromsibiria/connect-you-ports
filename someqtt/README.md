# Rust-based TCP Tunnel: Client-Server Application for Forwarding TCP and UDP Traffic

This project implements a high-performance client-server application that establishes a secure **TCP tunnel** between a local client and a remote server. Once the TCP connection is established, the tunnel can forward multiple local TCP connections and UDP traffic streams to remote destinations.

## The project can be used as a tunnel into the cloud infrastructure for development

You can forward multiple TCP and UDP connections through the persistent TCP tunnel, launch several infrastructure services without direct access, and define named enumerations for each service: `TCP_SOCKETS='db:127.0.0.1:5432;dev-api:127.0.0.1:8080;rabbit:127.0.0.1:5672'` `UDP_SOCKETS='iperf-udp:0.0.0.0:9092;dns:0.0.0.0:5553'`

On the client side, all these sockets are accessible locally. On the server side, connections are established to the appropriate services based on the target configuration, e.g.: `SERVER_TCP_TARGET='db:host-in-cloud-2:5432;dev-api:host-in-cloud-3:8080;rabbit:host-in-cloud-4:5672'` and for the UDP sockets: `SERVER_UDP_TARGET='iperf-udp:0.0.0.0:9092;dns:8.8.8.8:53'`

## Command line options

Values may be given as a separate argument (`--workers 8`) or inline (`--workers=8`). Environment variables remain the fallback source when an option is not set. See `--help` for full usage.

| Option | Description |
| --- | --- |
| `--server` | run as server (without this flag the app runs as a client) |
| `--workers <N>` | tokio worker threads; 0 or unset means auto-detect CPU cores |
| `--genkey [N]` | generate a random cipher key of N bytes in hex and exit (default: 32, AES-256); set it as `CRYPTO_KEY` on both sides |
| `--stat` | read statistics (file `STAT_FILE`, default /tmp/stat.txt; `'memory'` reads the in-process buffer) and print them as a table |
| `-h`, `--help` | print the help message with options and environment variables and exit |

## Environment variables (alphabetical)

| Variable | Description |
| --- | --- |
| `ALLOW_NET` | allowed networks list, CIDR entries separated by ';' (server mode); empty means no restriction |
| `CONNECTION_IDLE_LIMIT` | TCP connection idle timeout in seconds (default: 180) |
| `CRYPTO_KEY` | cipher key in hex, AES-256-GCM requires 32 bytes; generate with `--genkey` |
| `LOADING_LEVEL` | loading level: default, low, high, extremely (default: default) |
| `READ_BUFFER_SIZE` | read buffer size in bytes; below 1024 uses the loading-level default |
| `SERVER` | run as server when set to on/yes/true/ok/1, otherwise runs as a client |
| `SERVER_HOST` | listen address for the server, target host for the client (default: 0.0.0.0) |
| `SERVER_PORT` | server port (default: 1883) |
| `SERVER_TCP_TARGET` | TCP targets map 'name:ip:port;...' on the server side |
| `SERVER_UDP_TARGET` | UDP targets map 'name:ip:port;...' on the server side |
| `STAT_FILE` | statistics file path (default: /tmp/stat.txt); use `memory` to keep stats in process memory instead of a file |
| `STAT_SAVE_INTERVAL` | stats save interval in seconds (server default: 2, client default: 1) |
| `STAT_SHOW_INTERVAL` | stats display interval in seconds (default: 180) |
| `TCP_SOCKETS` | local TCP sockets map 'name:ip:port;...' forwarded through the tunnel (client side) |
| `UDP_BIND_FROM` | source address for outbound UDP on the server side (default: 0.0.0.0:0) |
| `UDP_CONNECTION_IDLE_LIMIT` | UDP idle timeout in seconds (default: 120) |
| `UDP_SOCKETS` | local UDP sockets map 'name:ip:port;...' forwarded through the tunnel (client side) |
| `WORKERS` | tokio worker threads; 0 or unset means auto-detect CPU cores |

## Creating examples with make

To create ready-to-run `docker-compose.yml` files, use:

```bash
make example_server   # generates docker-compose.yml for the server side
make example_client   # generates docker-compose.yml for the client side
```

Each target copies the corresponding template from `example/`, fills in a random `CRYPTO_KEY`, and prints the key plus the lines you need to edit (e.g. your server address, targets). Set the printed key as `CRYPTO_KEY` on the opposite side, then run:

```bash
docker compose up -d --build
```

## LLM-assisted development

Local LLM models (`qwen3.8:27b`, `muse-glimmer:30b`) were used during development in a moderate and careful manner - for boilerplate parts like the `help` message, doing it by hand is no longer appealing. Even so, I still strive to keep the code easy to read and comfortable for humans.
