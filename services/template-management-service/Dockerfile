# Build context — КОРЕНЬ РЕПОЗИТОРИЯ (тот же layout, что у policy-service/Dockerfile):
#   docker build --secret id=extra_ca_cert,src=infra/docker/unitel-root-ca.crt \
#     -f services/template-management-service/Dockerfile .

FROM rust:1.90-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka-dev pkg-config cmake protobuf-compiler libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY services/template-management-service .
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
COPY --from=builder /build/target/release/template-management-service /app/
COPY --from=builder /config_schemas /config_schemas
EXPOSE 8080 9090
# Non-root — same UID:GID convention as the Java (eclipse-temurin) and
# nginx (frontend) Dockerfiles in this repo; distroless Go images use
# their own baked-in :nonroot tag (uid 65532) instead.
RUN groupadd --system --gid 10001 mpp \
    && useradd --system --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin mpp
USER 10001:10001
ENTRYPOINT ["/app/template-management-service"]
