FROM rust:1.93
RUN apt update
RUN apt install -y openssl build-essential libssl-dev curl iperf bash

WORKDIR /usr/src/connection-service
COPY . .
RUN cargo install --path .
RUN du -h /usr/local/cargo/bin/connect-you-ports
RUN echo "connect-you-ports & iperf -s 0.0.0.0 -p 9091 & iperf -u -s 0.0.0.0 -p 9092" > /opt/run.sh && chmod 770 /opt/run.sh

RUN apt clean && rm -rf /var/lib/apt/lists/*

CMD ["bash", "-c", "/opt/run.sh"]
