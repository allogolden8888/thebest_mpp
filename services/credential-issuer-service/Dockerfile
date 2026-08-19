# Build context — эта директория:
#   docker build -f services/credential-issuer-service/Dockerfile services/credential-issuer-service

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
RUN CGO_ENABLED=0 go build -o /build/bin/credential-issuer-service ./cmd/credential-issuer-service

FROM gcr.io/distroless/static-debian12
COPY --from=builder /build/bin/credential-issuer-service /app/credential-issuer-service
EXPOSE 9090 9000
ENTRYPOINT ["/app/credential-issuer-service"]
