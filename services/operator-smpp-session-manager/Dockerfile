# Build context — корень репозитория (protobuf генерируется protobuf-maven-plugin
# при сборке, нужен platform-contracts/, см. README.md "Регенерация protobuf"):
#   docker build -f services/operator-smpp-session-manager/Dockerfile -t operator-smpp-session-manager ../..
#
# Не собран и не прогнан в этом окружении — Docker daemon недоступен.

FROM maven:3.9-eclipse-temurin-25 AS builder
WORKDIR /build
COPY platform-contracts /platform-contracts
COPY services/operator-smpp-session-manager/pom.xml .
RUN mvn -B -o dependency:go-offline || true
COPY services/operator-smpp-session-manager/src ./src
RUN mvn -B -Dplatform-contracts.dir=/platform-contracts -DskipTests package

FROM eclipse-temurin:25-jre-jammy
COPY --from=builder /build/target/operator-smpp-session-manager.jar /app/app.jar
EXPOSE 9090 9000
ENTRYPOINT ["java", "-jar", "/app/app.jar"]