# Build context — эта директория (сгенерированный protobuf-код уже
# закоммичен в internal/proto/gen/, см. README.md "Регенерация protobuf"):
#   docker build -f services/scheduler-critical-sweep/Dockerfile services/scheduler-critical-sweep
#
# Не собран и не прогнан в этом окружении — Docker daemon недоступен в этой
# песочнице (development_plan.md "Координация" п.5).

FROM golang:1.26-bookworm AS builder
WORKDIR /build
COPY go.mod go.sum ./
COPY internal/proto/gen/mpp/platformcontracts/go.mod internal/proto/gen/mpp/platformcontracts/go.sum internal/proto/gen/mpp/platformcontracts/
RUN --mount=type=secret,id=extra_ca_cert,required=false \
    if [ -s /run/secrets/extra_ca_cert ]; then \
        cp /run/secrets/extra_ca_cert /usr/local/share/ca-certificates/extra-ca.crt && update-ca-certificates; \
    fi && \
    go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /build/bin/scheduler-critical-sweep ./cmd/scheduler-critical-sweep

FROM gcr.io/distroless/static-debian12
COPY --from=builder /build/bin/scheduler-critical-sweep /app/scheduler-critical-sweep
EXPOSE 9090
ENTRYPOINT ["/app/scheduler-critical-sweep"]
