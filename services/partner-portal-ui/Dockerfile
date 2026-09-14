# Build context — эта директория, тот же паттерн, что
# services/backoffice-ui/Dockerfile:
#   docker build --secret id=extra_ca_cert,src=infra/docker/unitel-root-ca.crt \
#     -f services/partner-portal-ui/Dockerfile services/partner-portal-ui

FROM node:24-bookworm-slim AS builder
WORKDIR /build
COPY package.json package-lock.json ./
RUN --mount=type=secret,id=extra_ca_cert,required=false \
    if [ -s /run/secrets/extra_ca_cert ]; then \
        apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
        cp /run/secrets/extra_ca_cert /usr/local/share/ca-certificates/extra-ca.crt && update-ca-certificates && \
        export NODE_EXTRA_CA_CERTS=/run/secrets/extra_ca_cert; \
    fi; \
    npm ci --legacy-peer-deps
COPY . .
RUN npm run build

FROM nginx:1.27-alpine
COPY --from=builder /build/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 8080
# Non-root — see services/backoffice-ui/Dockerfile for the full rationale
# (verified live: nginx:alpine's /var/cache/nginx and /run/nginx.pid are not
# writable by its own non-root "nginx" user, fixed by repointing pid at /tmp
# and chown'ing the cache dir).
RUN addgroup -g 10001 -S mpp \
    && adduser -u 10001 -S -D -H -G mpp mpp \
    && sed -i 's#/run/nginx.pid#/tmp/nginx.pid#' /etc/nginx/nginx.conf \
    && chown -R mpp:mpp /var/cache/nginx
USER 10001:10001
