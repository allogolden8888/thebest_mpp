# Build context — корень репозитория (protobuf генерируется protobuf-maven-plugin
# при сборке, нужен platform-contracts/):
#   docker build -f services/delivery-reconciliation-service/Dockerfile -t delivery-reconciliation-service ../..
#
# Не собран и не прогнан в этом окружении — Docker daemon недоступен.

FROM maven:3.9-eclipse-temurin-25 AS builder
WORKDIR /build
COPY platform-contracts /platform-contracts
COPY services/delivery-reconciliation-service/pom.xml .
RUN mvn -B -o dependency:go-offline || true
COPY services/delivery-reconciliation-service/src ./src
RUN mvn -B -Dplatform-contracts.dir=/platform-contracts -DskipTests package

FROM eclipse-temurin:25-jre-jammy
COPY --from=builder /build/target/delivery-reconciliation-service.jar /app/app.jar
EXPOSE 9090
ENTRYPOINT ["java", "-jar", "/app/app.jar"]