# Build context — корень репозитория:
#   docker build -f services/billing-reconciliation/Dockerfile -t billing-reconciliation ../..
#
# Не собран и не прогнан в этом окружении — Docker daemon недоступен.

FROM maven:3.9-eclipse-temurin-25 AS builder
WORKDIR /build
COPY platform-contracts /platform-contracts
COPY services/billing-reconciliation/pom.xml .
RUN mvn -B -o dependency:go-offline || true
COPY services/billing-reconciliation/src ./src
RUN mvn -B -Dplatform-contracts.dir=/platform-contracts -DskipTests package

FROM eclipse-temurin:25-jre-jammy
COPY --from=builder /build/target/billing-reconciliation.jar /app/app.jar
EXPOSE 9090
ENTRYPOINT ["java", "-jar", "/app/app.jar"]