# Build context — КОРЕНЬ РЕПОЗИТОРИЯ (см. services/destination-resolution-service/Dockerfile
# для находки про rust:1.82 vs edition2024 и опциональный extra CA-сертификат):
#   docker build --secret id=extra_ca_cert,src=infra/docker/unitel-root-ca.crt \
#     -f services/routing-service/Dockerfile .
# Реально собран и провалидирован `docker build`.

FROM rust:1.90-bookworm AS builder
# libprotobuf-dev — см. services/destination-resolution-service/Dockerfile:
# --no-install-recommends отключает автоподтягивание пакета с
# /usr/include/google/protobuf/*.proto, без него protoc не находит
# google.protobuf.Timestamp и т.п.
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka-dev pkg-config cmake protobuf-compiler libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY services/routing-service .
COPY platform-contracts /platform-contracts
COPY config_schemas /config_schemas
ENV PLATFORM_CONTRACTS_DIR=/platform-contracts
RUN --mount=type=secret,id=extra_ca_cert,required=false \
    if [ -s /run/secrets/extra_ca_cert ]; then \
        cp /run/secrets/extra_ca_cert /usr/local/share/ca-certificates/extra-ca.crt && update-ca-certificates; \
    fi && \
    cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka1 ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /build/target/release/routing-service /app/
ENV ROUTE_TABLE_PATH=/config_schemas/examples/routing_table.valid.json
EXPOSE 9090
ENTRYPOINT ["/app/routing-service"]
