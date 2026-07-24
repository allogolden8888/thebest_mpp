# Build context — корень репозитория:
#   docker build -f services/billing-outbox-publisher/Dockerfile -t billing-outbox-publisher ../..
#
# Не собран и не прогнан в этом окружении — Docker daemon недоступен.

FROM maven:3.9-eclipse-temurin-25 AS builder
WORKDIR /build
COPY platform-contracts /platform-contracts
COPY services/billing-outbox-publisher/pom.xml .
RUN mvn -B -o dependency:go-offline || true
COPY services/billing-outbox-publisher/src ./src
RUN mvn -B -Dplatform-contracts.dir=/platform-contracts -DskipTests package

FROM eclipse-temurin:25-jre-jammy
COPY --from=builder /build/target/billing-outbox-publisher.jar /app/app.jar
EXPOSE 9090
ENTRYPOINT ["java", "-jar", "/app/app.jar"]