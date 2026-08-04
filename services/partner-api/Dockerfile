# Build context — эта директория:
#   docker build -f services/partner-api/Dockerfile services/partner-api
#
# Не собран и не прогнан в этом окружении — Docker daemon недоступен в этой
# песочнице (development_plan.md "Координация" п.5). Написан по стандартному
# multi-stage паттерну и сверен с реальными зависимостями go.mod, но
# `docker build` здесь не выполнялся.

FROM golang:1.26-bookworm AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /build/bin/partner-api ./cmd/partner-api

FROM gcr.io/distroless/static-debian12
COPY --from=builder /build/bin/partner-api /app/partner-api
EXPOSE 9090 8080
ENTRYPOINT ["/app/partner-api"]
