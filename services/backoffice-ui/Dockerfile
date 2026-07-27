# Build context — эта директория:
#   docker build -f services/backoffice-ui/Dockerfile services/backoffice-ui
#
# Не собран и не прогнан в этом окружении — Docker daemon недоступен в этой
# песочнице (development_plan.md "Координация" п.5). npm ci/npm run build
# реально выполнялись локально (не в Docker) — см. README.

FROM node:24-bookworm-slim AS builder
WORKDIR /build
COPY package.json package-lock.json ./
RUN npm ci --legacy-peer-deps
COPY . .
RUN npm run build

FROM nginx:1.27-alpine
COPY --from=builder /build/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 8080
