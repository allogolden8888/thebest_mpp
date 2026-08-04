# Build context — эта директория. protobuf/grpc-код генерируется
# protobuf-maven-plugin ПРИ СБОРКЕ (не закоммичен — в отличие от Go-сервисов
# в этом репо, см. README.md "Регенерация protobuf"), поэтому build context
# должен включать platform-contracts/ (два уровня выше):
#   docker build -f services/partner-smpp-gateway/Dockerfile -t partner-smpp-gateway ../..
#   (контекст — корень репозитория, не эта директория)
#
# Не собран и не прогнан в этом окружении — Docker daemon недоступен в этой
# песочнице (development_plan.md "Координация" п.5).

FROM maven:3.9-eclipse-temurin-25 AS builder
WORKDIR /build
COPY platform-contracts /platform-contracts
COPY services/partner-smpp-gateway/pom.xml .
RUN mvn -B -o dependency:go-offline || true
COPY services/partner-smpp-gateway/src ./src
RUN mvn -B -Dplatform-contracts.dir=/platform-contracts -DskipTests package

FROM eclipse-temurin:25-jre-jammy
COPY --from=builder /build/target/partner-smpp-gateway.jar /app/app.jar
EXPOSE 9090 9000 2775
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
