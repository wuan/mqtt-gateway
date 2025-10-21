FROM debian:bookworm-slim@sha256:78d2f66e0fec9e5a39fb2c72ea5e052b548df75602b5215ed01a17171529f706

RUN apt update
RUN apt install -y openssl

COPY target/release/mqtt-gateway ./
RUN chmod a+x mqtt-gateway

VOLUME /config

ENV RUST_LOG=info
CMD ["./mqtt-gateway"]