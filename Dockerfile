FROM debian:bookworm-slim@sha256:7b140f374b289a7c2befc338f42ebe6441b7ea838a042bbd5acbfca6ec875818

RUN apt update
RUN apt install -y openssl ca-certificates

COPY target/release/mqtt-gateway ./
RUN chmod a+x mqtt-gateway

VOLUME /config

ENV RUST_LOG=info
CMD ["./mqtt-gateway"]