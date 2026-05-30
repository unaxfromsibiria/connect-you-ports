
# Forwarding TCP connections and UDP transfers via Redis pub-sub

This project enables you to create a local server for forwarding both TCP connections and UDP transfers. On the server side, it establishes persistent connections to remote destinations with encryption support. The solution delivers excellent performance and reliability. A similar [project in Python here](https://github.com/unaxfromsibiria/aiomqttbridge) uses MQTT for message brokering. While MQTT generally offers better latency and throughput characteristics, this Rust implementation provides significant performance improvements and resource efficiency. The configuration of services is similar in both implementations.

The Rust implementation brings substantial savings in memory and CPU usage compared to the Python version, making it particularly suitable for high-load scenarios. While MQTT typically performs better in terms of latency and traffic handling, this Redis-based solution offers an alternative approach with its own advantages in certain use cases. Note: next steap is using Rust + MQTT.

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
