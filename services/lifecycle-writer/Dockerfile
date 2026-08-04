# Build context — эта директория:
#   docker build -f services/lifecycle-writer/Dockerfile services/lifecycle-writer
#
# Не собран и не прогнан в этом окружении — Docker daemon недоступен.

FROM golang:1.26-bookworm AS builder
WORKDIR /build
COPY go.mod go.sum ./
COPY internal/proto/gen/mpp/platformcontracts/go.mod internal/proto/gen/mpp/platformcontracts/go.sum internal/proto/gen/mpp/platformcontracts/
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /build/bin/lifecycle-writer ./cmd/lifecycle-writer

FROM gcr.io/distroless/static-debian12
COPY --from=builder /build/bin/lifecycle-writer /app/lifecycle-writer
EXPOSE 9090
ENTRYPOINT ["/app/lifecycle-writer"]