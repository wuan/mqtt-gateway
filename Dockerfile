FROM debian:bookworm-slim

COPY target/release/mqtt-gateway ./
RUN chmod a+x mqtt-gateway

VOLUME /config

CMD ["./mqtt-gateway"]