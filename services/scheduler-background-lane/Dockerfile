# Build context — эта директория (protobuf-код закоммичен в
# src/main/java/uz/mpp/platformcontracts/, см. README.md "Регенерация protobuf"):
#   docker build -f services/scheduler-background-lane/Dockerfile services/scheduler-background-lane
#
# Не собран и не прогнан в этом окружении — Docker daemon недоступен.

FROM maven:3.9-eclipse-temurin-25 AS builder
WORKDIR /build
COPY pom.xml .
RUN mvn -B dependency:go-offline
COPY src ./src
RUN mvn -B -DskipTests package

FROM eclipse-temurin:25-jre-jammy
COPY --from=builder /build/target/scheduler-background-lane.jar /app/app.jar
EXPOSE 9090
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
