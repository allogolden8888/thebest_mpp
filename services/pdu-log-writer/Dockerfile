# Build context — эта директория:
#   docker build -f services/pdu-log-writer/Dockerfile services/pdu-log-writer

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
RUN CGO_ENABLED=0 go build -o /build/bin/pdu-log-writer ./cmd/pdu-log-writer

FROM gcr.io/distroless/static-debian12
COPY --from=builder /build/bin/pdu-log-writer /app/pdu-log-writer
EXPOSE 9090
ENTRYPOINT ["/app/pdu-log-writer"]
