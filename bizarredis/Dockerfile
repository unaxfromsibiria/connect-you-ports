FROM rust:1.95 AS builder

RUN apt update
RUN apt install -y openssl build-essential libssl-dev pkg-config
WORKDIR /usr/src/connection-service
COPY . .
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN apt update
RUN apt install -y iperf bash libssl3 curl procps
RUN useradd -m -s /bin/bash appuser
WORKDIR /home/appuser
COPY --from=builder /usr/src/connection-service/target/release/bizarredis .
RUN echo "./bizarredis & iperf -s -p 9093 & iperf -u -s -p 9094" > /opt/run.sh
RUN chmod +x /opt/run.sh
RUN chown appuser:appuser /opt/run.sh
USER appuser
CMD ["bash", "-c", "/opt/run.sh"]
