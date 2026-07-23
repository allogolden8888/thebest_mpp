# Build context — КОРЕНЬ РЕПОЗИТОРИЯ (см. services/destination-resolution-service/Dockerfile).
#   docker build -f services/routing-service/Dockerfile .
# Не собран и не прогнан в этом окружении — тот же недоступный Docker daemon.

FROM rust:1.82-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka-dev pkg-config cmake \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY services/routing-service .
COPY platform-contracts /platform-contracts
COPY config_schemas /config_schemas
ENV PLATFORM_CONTRACTS_DIR=/platform-contracts
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka1 ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /build/target/release/routing-service /app/
ENV ROUTE_TABLE_PATH=/config_schemas/examples/routing_table.valid.json
EXPOSE 9090
ENTRYPOINT ["/app/routing-service"]
