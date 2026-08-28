# Forwarding TCP connections and UDP transfers from cloud infrastructure to local OS

This project consists of two subprojects with the same purpose:

- One uses a real Redis server and its client. This is the older, less-tested implementation, as an alternative existed with a different transport [via MQTT](https://github.com/unaxfromsibiria/connect-you-ports-mq). The MQTT version performed better.

- The second, more stable implementation with built-in connection handling is called `bizarredis`. Try using this one.

- Server of bizarredis for Android.

- SoMeQTT - another implementation of the same kind with improved performance and reduced losses (`cd someqtt`)
