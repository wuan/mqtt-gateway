FROM debian:bookworm-slim@sha256:b1211f6d19afd012477bd34fdcabb6b663d680e0f4b0537da6e6b0fd057a3ec3

RUN apt update
RUN apt upgrade -y
RUN apt install -y openssl

COPY target/release/mqtt-gateway ./
RUN chmod a+x mqtt-gateway

VOLUME /config

ENV RUST_LOG=info
CMD ["./mqtt-gateway"]