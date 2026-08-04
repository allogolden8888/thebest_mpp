# Build context — эта директория:
#   docker build -f services/replay-service/Dockerfile services/replay-service
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
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /build/bin/replay-service ./cmd/replay-service

FROM gcr.io/distroless/static-debian12
COPY --from=builder /build/bin/replay-service /app/replay-service
EXPOSE 9090 9000
ENTRYPOINT ["/app/replay-service"]