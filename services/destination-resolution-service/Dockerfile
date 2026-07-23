# Build context — КОРЕНЬ РЕПОЗИТОРИЯ, не эта директория (монорепо: build.rs
# нужен platform-contracts/, лежащий на два уровня выше сервиса):
#   docker build -f services/destination-resolution-service/Dockerfile .
#
# Не собран и не прогнан в этом окружении — тот же Docker daemon
# недоступен, что помешал поднять PostgreSQL через docker в начале сессии
# (migrations/README.md). Написан по стандартному multi-stage паттерну и
# сверен с реальными зависимостями Cargo.toml (librdkafka runtime), но
# `docker build` здесь не выполнялся — единственный артефакт в этой
# директории, не провалидированный запуском, в отличие от cargo build/test.

FROM rust:1.82-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka-dev pkg-config cmake \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY services/destination-resolution-service .
COPY platform-contracts /platform-contracts
ENV PLATFORM_CONTRACTS_DIR=/platform-contracts
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka1 ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /build/target/release/destination-resolution-service /app/
COPY --from=builder /build/data /app/data
EXPOSE 9090
ENTRYPOINT ["/app/destination-resolution-service"]
