# Build context — КОРЕНЬ РЕПОЗИТОРИЯ, не эта директория (монорепо: build.rs
# нужен platform-contracts/, лежащий на два уровня выше сервиса):
#   docker build -f services/destination-resolution-service/Dockerfile .
#
# Реально собран и провалидирован `docker build` (Docker Desktop стал
# доступен только позже сессии). Найдена и исправлена реальная находка:
# `FROM rust:1.82-bookworm` не мог собрать `Cargo.toml` этого сервиса —
# `edition = "2024"` требует Cargo ≥1.85 ("feature edition2024 is
# required"), эта ошибка физически не могла всплыть раньше, пока `docker
# build` не был проверен вживую (host'овый `cargo build`/`test` работал,
# т.к. локальный тулчейн 1.97 — новее). Поднято до `rust:1.90-bookworm`
# (тот же паттерн у всех 5 Rust-сервисов).
#
# Опциональный extra CA-сертификат для сети с TLS-инспектирующим прокси —
# см. infra/docker/README.md, по умолчанию не используется:
#   docker build --secret id=extra_ca_cert,src=infra/docker/unitel-root-ca.crt \
#     -f services/destination-resolution-service/Dockerfile .

FROM rust:1.90-bookworm AS builder
# libprotobuf-dev — реальная находка: --no-install-recommends отключает
# автоматическое подтягивание этого пакета, а именно он несёт
# /usr/include/google/protobuf/*.proto (well-known types типа
# timestamp.proto) — одного protobuf-compiler (сам бинарник protoc)
# недостаточно, prost-build/protoc не находит импорты google.protobuf.*
# без него ("File not found"). build.rs дополнительно ищет WKT по
# известным путям (/usr/include и т.д.) и явно передаёт их как -I —
# страховка, не единственная защита.
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka-dev pkg-config cmake protobuf-compiler libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY services/destination-resolution-service .
COPY platform-contracts /platform-contracts
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
COPY --from=builder /build/target/release/destination-resolution-service /app/
COPY --from=builder /build/data /app/data
EXPOSE 9090
ENTRYPOINT ["/app/destination-resolution-service"]
