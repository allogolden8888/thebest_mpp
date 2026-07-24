# Build context — эта директория (protobuf-код уже сгенерирован и
# закоммичен в internal/proto/gen/, тот же паттерн, что у
# dlr-correlation-writer/Go-сервисов Субагента 1):
#   docker build -f services/dlr-manager/Dockerfile services/dlr-manager
#
# Не собран и не прогнан в этом окружении — Docker daemon недоступен.

FROM golang:1.26-bookworm AS builder
WORKDIR /build
COPY go.mod go.sum ./
COPY internal/proto/gen/mpp/platformcontracts/go.mod internal/proto/gen/mpp/platformcontracts/
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /build/bin/dlr-manager ./cmd/dlr-manager

FROM gcr.io/distroless/static-debian12
COPY --from=builder /build/bin/dlr-manager /app/dlr-manager
EXPOSE 9090
ENTRYPOINT ["/app/dlr-manager"]
