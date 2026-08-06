# Build context — эта директория (protobuf/grpc-код уже сгенерирован и
# закоммичен в internal/proto/gen/, тот же паттерн, что у dlr-correlation-writer/
# dlr-manager/Go-сервисов Субагента 1):
#   docker build -f services/partner-notification-service/Dockerfile services/partner-notification-service
#
# Не собран и не прогнан в этом окружении — Docker daemon недоступен.

FROM golang:1.26-bookworm AS builder
WORKDIR /build
COPY go.mod go.sum ./
COPY internal/proto/gen/mpp/platformcontracts/go.mod internal/proto/gen/mpp/platformcontracts/
RUN --mount=type=secret,id=extra_ca_cert,required=false \
    if [ -s /run/secrets/extra_ca_cert ]; then \
        cp /run/secrets/extra_ca_cert /usr/local/share/ca-certificates/extra-ca.crt && update-ca-certificates; \
    fi && \
    go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /build/bin/partner-notification-service ./cmd/partner-notification-service

FROM gcr.io/distroless/static-debian12
COPY --from=builder /build/bin/partner-notification-service /app/partner-notification-service
# PARTNER_CONFIG_PATH — в этом срезе статический файл-заглушка (в проде
# читалось бы из config.changes/Configuration Redis, см. README) — build
# context этого Dockerfile не включает config_schemas/ (та же директория-
# only конвенция, что у dlr-correlation-writer/dlr-manager), файл должен
# быть примонтирован отдельно (ConfigMap) для реального запуска контейнера.
ENV PARTNER_CONFIG_PATH=/config/partner.valid.json
EXPOSE 9090
ENTRYPOINT ["/app/partner-notification-service"]
