# Build context — КОРЕНЬ РЕПОЗИТОРИЯ (см. services/destination-resolution-service/Dockerfile
# для полного обоснования монорепо-layout):
#   docker build -f services/policy-service/Dockerfile .
#
# Не собран и не прогнан в этом окружении — тот же недоступный Docker daemon,
# что и у остальных сервисов этой сессии.

FROM rust:1.82-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka-dev pkg-config cmake \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY services/policy-service .
COPY platform-contracts /platform-contracts
COPY config_schemas /config_schemas
ENV PLATFORM_CONTRACTS_DIR=/platform-contracts
ENV POLICY_RULESET_PATH=/config_schemas/examples/policy_ruleset.valid.json
ENV POLICY_TEMPLATE_PATH=/config_schemas/examples/policy_template.valid.json
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka1 ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /build/target/release/policy-service /app/
EXPOSE 9090
ENTRYPOINT ["/app/policy-service"]
