# Build context — эта директория (не корень репозитория — тот же паттерн,
# что у Go-сервисов Субагента 1: protobuf-код уже сгенерирован и закоммичен
# в internal/proto/gen/, Docker build не должен запускать protoc):
#   docker build -f services/dlr-correlation-writer/Dockerfile services/dlr-correlation-writer
#
# Не собран и не прогнан в этом окружении — Docker daemon недоступен.

FROM golang:1.26-bookworm AS builder
WORKDIR /build
COPY go.mod go.sum ./
COPY internal/proto/gen/mpp/platformcontracts/go.mod internal/proto/gen/mpp/platformcontracts/
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /build/bin/dlr-correlation-writer ./cmd/dlr-correlation-writer

FROM gcr.io/distroless/static-debian12
COPY --from=builder /build/bin/dlr-correlation-writer /app/dlr-correlation-writer
EXPOSE 9090
ENTRYPOINT ["/app/dlr-correlation-writer"]
