# Build context — эта директория (protobuf-код генерируется protoc заранее
# и закоммичен в src/main/java/uz/mpp/platformcontracts/, см. README.md
# "Регенерация protobuf" — не перегенерируется на этапе Docker build):
#   docker build -f services/scheduler-standard-lane/Dockerfile services/scheduler-standard-lane
#
# Не собран и не прогнан в этом окружении — Docker daemon недоступен в этой
# песочнице (development_plan.md "Координация" п.5).

FROM maven:3.9-eclipse-temurin-25 AS builder
WORKDIR /build
COPY pom.xml .
RUN mvn -B dependency:go-offline
COPY src ./src
RUN mvn -B -DskipTests package

FROM eclipse-temurin:25-jre-jammy
COPY --from=builder /build/target/scheduler-standard-lane.jar /app/app.jar
EXPOSE 9090
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
