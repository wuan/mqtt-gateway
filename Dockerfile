FROM debian:bookworm-slim@sha256:7ff6ba31527b9e336162fa061746c366c4b2032f4d2cff2078bea43f78efe3f8

RUN apt-get update \
    && apt-get install -y --no-install-recommends openssl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY target/release/mqtt-gateway ./mqtt-gateway
RUN chmod a+x ./mqtt-gateway

# Run as a non-root user. A numeric UID avoids depending on user-management
# tooling that is not present in the minimal base image.
USER 10001:10001

# The gateway looks for config.yml in the working directory or in ./config
# (i.e. /config/config.yml, the mounted volume).
WORKDIR /
VOLUME /config

ENV RUST_LOG=info
STOPSIGNAL SIGTERM
# Liveness only, as there is no HTTP readiness endpoint yet.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s \
    CMD ["sh", "-c", "kill -0 1"]

CMD ["./mqtt-gateway"]