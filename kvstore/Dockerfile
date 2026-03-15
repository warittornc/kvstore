FROM rust:1.94-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY kvstore ./kvstore
COPY router ./router

RUN cargo build --release -p kvstore

FROM debian:bookworm-slim

COPY --from=builder /app/target/release/kvstore /usr/local/bin/kvstore

CMD ["kvstore"]
