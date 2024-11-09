FROM debian:bookworm-slim

COPY target/release/mqtt-gateway ./

VOLUME /config

CMD ["./mqtt-gateway"]