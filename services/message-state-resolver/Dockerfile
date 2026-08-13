# Build context — КОРЕНЬ РЕПОЗИТОРИЯ (см. services/destination-resolution-service/Dockerfile).
#   docker build -f services/message-state-resolver/Dockerfile .
#
# Реально собран и провалидирован `docker build` (с --secret id=extra_ca_cert,
# см. infra/docker/README.md).

FROM eclipse-temurin:25-jdk AS builder
# Опциональный extra CA-сертификат — см. maven-based Dockerfile'ы этой сессии
# (например services/billing-ledger-writer/Dockerfile) за полным обоснованием.
RUN --mount=type=secret,id=extra_ca_cert,required=false \
    if [ -s /run/secrets/extra_ca_cert ]; then \
        cp /run/secrets/extra_ca_cert /usr/local/share/ca-certificates/extra-ca.crt && update-ca-certificates && \
        keytool -importcert -noprompt -alias extra-ca -file /run/secrets/extra_ca_cert -keystore "$JAVA_HOME/lib/security/cacerts" -storepass changeit; \
    fi
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
ENTRYPOINT ["java", "-XX:StartFlightRecording=filename=/tmp/message-state-resolver.jfr,duration=150s,dumponexit=true,settings=profile", "-jar", "/app/message-state-resolver.jar"]
