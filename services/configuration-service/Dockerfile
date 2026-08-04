# Build context — эта директория:
#   docker build -f services/configuration-service/Dockerfile services/configuration-service
#
# Не собран и не прогнан в этом окружении — Docker daemon недоступен.

FROM golang:1.26-bookworm AS builder
WORKDIR /build
COPY go.mod go.sum ./
COPY internal/proto/gen/mpp/platformcontracts/go.mod internal/proto/gen/mpp/platformcontracts/go.sum internal/proto/gen/mpp/platformcontracts/
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /build/bin/configuration-service ./cmd/configuration-service

FROM gcr.io/distroless/static-debian12
COPY --from=builder /build/bin/configuration-service /app/configuration-service
EXPOSE 9090 9000
ENTRYPOINT ["/app/configuration-service"]