# Build context — эта директория:
#   docker build --secret id=extra_ca_cert,src=infra/docker/unitel-root-ca.crt \
#     -f services/backoffice-ui/Dockerfile services/backoffice-ui
#
# Реально собран и провалидирован `docker build`. npm ci/npm run build также
# реально выполнялись локально (не в Docker) — см. README.

FROM node:24-bookworm-slim AS builder
WORKDIR /build
COPY package.json package-lock.json ./
# Опциональный extra CA-сертификат — см. infra/docker/README.md. Node не
# использует системный CA-store автоматически (свой бандл, как и JVM) —
# NODE_EXTRA_CA_CERTS, не только update-ca-certificates. Реальная находка:
# в отличие от golang/rust/eclipse-temurin образов этой сессии,
# node:24-bookworm-slim не несёт пакет ca-certificates вообще (нет и
# /usr/local/share/ca-certificates/, и самого update-ca-certificates) —
# ставим explicitly, не полагаемся на то, что он уже есть в базовом образе.
RUN --mount=type=secret,id=extra_ca_cert,required=false \
    if [ -s /run/secrets/extra_ca_cert ]; then \
        apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
        cp /run/secrets/extra_ca_cert /usr/local/share/ca-certificates/extra-ca.crt && update-ca-certificates && \
        export NODE_EXTRA_CA_CERTS=/run/secrets/extra_ca_cert; \
    fi; \
    npm ci --legacy-peer-deps
COPY . .
RUN npm run build

FROM nginx:1.27-alpine
COPY --from=builder /build/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 8080
