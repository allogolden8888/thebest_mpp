# Build context — эта директория (protobuf-код уже сгенерирован и
# закоммичен в internal/proto/gen/, тот же паттерн, что у
# dlr-correlation-writer/Go-сервисов Субагента 1):
#   docker build -f services/dlr-manager/Dockerfile services/dlr-manager
#
# Опциональный extra CA-сертификат для сетей с TLS-инспектирующим прокси
# (см. infra/docker/README.md) — по умолчанию НЕ используется, ничего не
# меняется в обычной сборке:
#   docker build --secret id=extra_ca_cert,src=infra/docker/unitel-root-ca.crt \
#     -f services/dlr-manager/Dockerfile services/dlr-manager

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
RUN CGO_ENABLED=0 go build -o /build/bin/dlr-manager ./cmd/dlr-manager

FROM gcr.io/distroless/static-debian12
COPY --from=builder /build/bin/dlr-manager /app/dlr-manager
EXPOSE 9090
ENTRYPOINT ["/app/dlr-manager"]
