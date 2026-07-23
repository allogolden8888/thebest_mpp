# Build context — КОРЕНЬ РЕПОЗИТОРИЯ (см. services/destination-resolution-service/Dockerfile
# для полного обоснования монорепо-layout):
#   docker build -f services/billing-service/Dockerfile .
#
# Не собран и не прогнан в этом окружении — тот же недоступный Docker daemon,
# что и у остальных сервисов этой сессии.

FROM eclipse-temurin:25-jdk AS builder
RUN apt-get update && apt-get install -y --no-install-recommends maven protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY services/billing-service .
COPY platform-contracts /platform-contracts
COPY config_schemas /config_schemas
RUN mvn -q -Dprotobuf.protocExecutable=/usr/bin/protoc package -DskipTests

FROM eclipse-temurin:25-jre
WORKDIR /app
COPY --from=builder /build/target/billing-service-0.1.0.jar /app/billing-service.jar
ENV BILLING_TARIFF_PATH=/config_schemas/examples/billing_tariff.valid.json
EXPOSE 9090
ENTRYPOINT ["java", "-jar", "/app/billing-service.jar"]
