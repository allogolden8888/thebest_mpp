# Build context — корень репозитория (protobuf генерируется protobuf-maven-plugin
# при сборке, нужен platform-contracts/, см. README.md "Регенерация protobuf"):
#   docker build -f services/operator-smpp-session-manager/Dockerfile -t operator-smpp-session-manager ../..
#
# Реально собран и провалидирован `docker build` (с --secret id=extra_ca_cert,
# см. infra/docker/README.md).

FROM maven:3.9-eclipse-temurin-25 AS builder
WORKDIR /build
# Опциональный extra CA-сертификат для сети с TLS-инспектирующим прокси —
# см. infra/docker/README.md, по умолчанию не используется. JVM не использует
# системный CA-store автоматически (тот же нюанс, что был найден для mvn/JDK
# на хосте, см. services/billing-service/README.md) — нужен и update-ca-certificates
# (общий TLS), и отдельный keytool-импорт в cacerts (для Maven/JVM).
RUN --mount=type=secret,id=extra_ca_cert,required=false \
    if [ -s /run/secrets/extra_ca_cert ]; then \
        cp /run/secrets/extra_ca_cert /usr/local/share/ca-certificates/extra-ca.crt && update-ca-certificates && \
        keytool -importcert -noprompt -alias extra-ca -file /run/secrets/extra_ca_cert -keystore "$JAVA_HOME/lib/security/cacerts" -storepass changeit; \
    fi
COPY platform-contracts /platform-contracts
COPY services/operator-smpp-session-manager/pom.xml .
RUN mvn -B -o dependency:go-offline || true
COPY services/operator-smpp-session-manager/src ./src
RUN mvn -B -Dplatform-contracts.dir=/platform-contracts -DskipTests package

FROM eclipse-temurin:25-jre-jammy
COPY --from=builder /build/target/operator-smpp-session-manager.jar /app/app.jar
EXPOSE 9090 9000
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-XX:InitialRAMPercentage=50", "-XX:StartFlightRecording=filename=/tmp/operator-smpp-session-manager.jfr,duration=150s,dumponexit=true,settings=profile", "-jar", "/app/app.jar"]