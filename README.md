# Forwarding TCP connections and UDP transfers from cloud infrastructure to local OS

All subprojects in this repository solve the same task: they create a persistent tunnel between a local client and a remote server so that multiple local TCP connections and UDP traffic streams can be forwarded into cloud infrastructure (databases, APIs, message brokers, ...) without direct access. Each subproject is an independent Cargo project (Rust) with its own README describing configuration and usage in detail.

## someqtt - the main implementation

**Use `someqtt`** (`cd someqtt`). It is the primary and best implementation: it has been in active use for a long time, is well tested, and offers improved performance and reduced losses compared to the older variants below. Unlike the Redis-based predecessors, it is self-contained (the client connects directly to the server, no external broker) and tunnels traffic over application-layer protocols - MQTT by default, with HTTP as an alternative transport.

- `someqtt` - the main network service (client + server). See [someqtt/README.md](someqtt/README.md) for options and environment variables.
- `someqtt-android-client` - a client-only build of the same service as a native library for Android, with a Kotlin UI template. See [someqtt-android-client/README.md](someqtt-android-client/README.md).

## Older implementations (kept for reference)

These subprojects serve the same purpose but are no longer maintained:

- `bizarredis` - an earlier implementation similar to someqtt; no longer current.
- `bizarredis-android-client` - its Android client.
- `redis-pubsub` - the oldest implementation, based on a real Redis server and pub-sub; superseded by the MQTT-based variants.

The root `docker-compose.yml` is a leftover from the redis-pubsub era and should not be used.
