# Build context — корень репозитория:
#   docker build -f services/billing-ledger-writer/Dockerfile -t billing-ledger-writer ../..
#
# Не собран и не прогнан в этом окружении — Docker daemon недоступен.

FROM maven:3.9-eclipse-temurin-25 AS builder
WORKDIR /build
COPY platform-contracts /platform-contracts
COPY services/billing-ledger-writer/pom.xml .
RUN mvn -B -o dependency:go-offline || true
COPY services/billing-ledger-writer/src ./src
RUN mvn -B -Dplatform-contracts.dir=/platform-contracts -DskipTests package

FROM eclipse-temurin:25-jre-jammy
COPY --from=builder /build/target/billing-ledger-writer.jar /app/app.jar
EXPOSE 9090
ENTRYPOINT ["java", "-jar", "/app/app.jar"]