FROM debian:bookworm-slim

RUN apt update
RUN apt install -y openssl

COPY target/release/mqtt-gateway ./
RUN chmod a+x mqtt-gateway

VOLUME /config

CMD ["./mqtt-gateway"]