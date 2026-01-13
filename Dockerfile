FROM debian:bookworm-slim@sha256:94c4d598b5987d76c38408657aae7118b101662595bf5eefe478e093a0bed2f6

RUN apt update
RUN apt install -y openssl

COPY target/release/mqtt-gateway ./
RUN chmod a+x mqtt-gateway

VOLUME /config

ENV RUST_LOG=info
CMD ["./mqtt-gateway"]