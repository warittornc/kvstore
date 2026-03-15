FROM rust:1.94-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY kvstore ./kvstore
COPY router ./router

RUN cargo build --release -p kvstore_router

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/kvstore_router /usr/local/bin/kvstore_router

CMD ["kvstore_router"]
