# Build context — эта директория:
#   docker build -f services/ops-visibility-service/Dockerfile services/ops-visibility-service

FROM golang:1.26-bookworm AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN --mount=type=secret,id=extra_ca_cert,required=false \
    if [ -s /run/secrets/extra_ca_cert ]; then \
        cp /run/secrets/extra_ca_cert /usr/local/share/ca-certificates/extra-ca.crt && update-ca-certificates; \
    fi && \
    go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /build/bin/ops-visibility-service ./cmd/ops-visibility-service

FROM gcr.io/distroless/static-debian12
COPY --from=builder /build/bin/ops-visibility-service /app/ops-visibility-service
EXPOSE 9090
ENTRYPOINT ["/app/ops-visibility-service"]
