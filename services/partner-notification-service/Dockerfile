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

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /build/bin/partner-notification-service /app/partner-notification-service
# Production defaults to Configuration Redis bootstrap plus a complete
# per-process config.changes mirror. Static input remains an explicit local
# escape hatch: PARTNER_CONFIG_MODE=file together with PARTNER_CONFIG_PATH.
EXPOSE 9090
USER nonroot:nonroot
ENTRYPOINT ["/app/partner-notification-service"]
