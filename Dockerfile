FROM debian:bookworm-slim

COPY target/release/mqtt-gateway ./

CMD ["./mqtt-gateway"]