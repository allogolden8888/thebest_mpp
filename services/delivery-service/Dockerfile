# Build context — КОРЕНЬ РЕПОЗИТОРИЯ (см. services/destination-resolution-service/Dockerfile).
#   docker build -f services/delivery-service/Dockerfile .
#
# Не собран и не прогнан в этом окружении — тот же недоступный Docker daemon,
# что и у остальных сервисов этой сессии.

FROM eclipse-temurin:25-jdk AS builder
RUN apt-get update && apt-get install -y --no-install-recommends maven protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY services/delivery-service .
COPY platform-contracts /platform-contracts
COPY config_schemas /config_schemas
# grpc.plugin.classifier — protoc-gen-grpc-java нативный бинарник, платформо-
# специфичный (см. README): linux-x86_64 для этого Linux-образа, не дефолтный
# osx-aarch_64 (локальная разработка на Apple Silicon).
RUN mvn -q -Dprotobuf.protocExecutable=/usr/bin/protoc -Dgrpc.plugin.classifier=linux-x86_64 package -DskipTests

FROM eclipse-temurin:25-jre
WORKDIR /app
COPY --from=builder /build/target/delivery-service-0.1.0.jar /app/delivery-service.jar
EXPOSE 9090
ENTRYPOINT ["java", "-jar", "/app/delivery-service.jar"]
