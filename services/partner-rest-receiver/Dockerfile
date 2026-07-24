# Build context — КОРЕНЬ РЕПОЗИТОРИЯ (см. services/destination-resolution-service/Dockerfile).
#   docker build -f services/partner-rest-receiver/Dockerfile .
# Не собран и не прогнан в этом окружении — недоступный Docker daemon
# (см. README/development_plan.md "Координация").

FROM rust:1.82-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka-dev pkg-config cmake \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY services/partner-rest-receiver .
COPY platform-contracts /platform-contracts
COPY config_schemas /config_schemas
ENV PLATFORM_CONTRACTS_DIR=/platform-contracts
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka1 ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /build/target/release/partner-rest-receiver /app/
ENV PARTNER_CONFIG_PATH=/config_schemas/examples/partner.valid.json
EXPOSE 9090 8080
ENTRYPOINT ["/app/partner-rest-receiver"]
