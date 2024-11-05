FROM rust:1.82

COPY target/release/mqtt-gateway ./

CMD ["./mqtt-gateway"]