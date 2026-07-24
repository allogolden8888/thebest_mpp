# Build context — КОРЕНЬ РЕПОЗИТОРИЯ (см. services/destination-resolution-service/Dockerfile).
#   docker build -f services/message-state-resolver/Dockerfile .
#
# Не собран и не прогнан в этом окружении — тот же недоступный Docker daemon,
# что и у остальных сервисов этой сессии.

FROM eclipse-temurin:25-jdk AS builder
RUN apt-get update && apt-get install -y --no-install-recommends maven protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY services/message-state-resolver .
COPY platform-contracts /platform-contracts
RUN mvn -q -Dprotobuf.protocExecutable=/usr/bin/protoc package -DskipTests

FROM eclipse-temurin:25-jre
WORKDIR /app
COPY --from=builder /build/target/message-state-resolver-0.1.0.jar /app/message-state-resolver.jar
EXPOSE 9090
ENTRYPOINT ["java", "-jar", "/app/message-state-resolver.jar"]
