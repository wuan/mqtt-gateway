FROM debian:bookworm-slim@sha256:66f78fcf318767c497d1991884619017549892bbfdc1d12f26a3d3ce9b1e30c9

RUN apt update
RUN apt install -y openssl

COPY target/release/mqtt-gateway ./
RUN chmod a+x mqtt-gateway

VOLUME /config

ENV RUST_LOG=info
CMD ["./mqtt-gateway"]