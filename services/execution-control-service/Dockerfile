# Build context — эта директория (в отличие от destination-resolution-service,
# .proto не компилируются на этапе Docker build — сгенерированный Go-код уже
# закоммичен в internal/proto/gen/, см. README.md "Регенерация protobuf"):
#   docker build -f services/execution-control-service/Dockerfile services/execution-control-service
#
# Не собран и не прогнан в этом окружении — Docker daemon недоступен в этой
# песочнице (development_plan.md "Координация" п.5, тот же приём, что и в
# services/destination-resolution-service/Dockerfile). Написан по стандартному
# multi-stage паттерну и сверен с реальными зависимостями go.mod, но
# `docker build` здесь не выполнялся.

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
RUN CGO_ENABLED=0 go build -o /build/bin/execution-control-service ./cmd/execution-control-service

FROM gcr.io/distroless/static-debian12
COPY --from=builder /build/bin/execution-control-service /app/execution-control-service
EXPOSE 9090 9000
ENTRYPOINT ["/app/execution-control-service"]
