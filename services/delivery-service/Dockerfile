# Build context — КОРЕНЬ РЕПОЗИТОРИЯ (см. services/destination-resolution-service/Dockerfile).
#   docker build -f services/delivery-service/Dockerfile .
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
COPY services/delivery-service .
COPY platform-contracts /platform-contracts
COPY config_schemas /config_schemas
# grpc.plugin.classifier — protoc-gen-grpc-java нативный бинарник,
# платформо-специфичный (см. README): нужен linux-x86_64/linux-aarch_64 для
# этого Linux-образа, не дефолтный osx-aarch_64 (локальная разработка на
# Apple Silicon вне контейнера).
#
# РЕАЛЬНАЯ НАХОДКА (docker build, впервые проверено вживую): жёстко
# прописанный linux-x86_64 ломался на Apple Silicon хосте — Docker Desktop
# по умолчанию собирает НАТИВНЫЙ arm64-контейнер (без эмуляции, быстро), а
# скачанный x86_64-плагин внутри такого контейнера пытается выполниться
# через Rosetta и падает ("rosetta error: failed to open elf"). Архитектура
# определяется динамически через uname -m внутри самого RUN — так плагин
# всегда совпадает с реальной архитектурой контейнера, независимо от того,
# на каком железе собирается образ (Apple Silicon локально или x86_64 в
# проде/CI), без явного --platform флага снаружи.
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "aarch64" ]; then GRPC_CLASSIFIER=linux-aarch_64; else GRPC_CLASSIFIER=linux-x86_64; fi && \
    mvn -q -Dprotobuf.protocExecutable=/usr/bin/protoc -Dgrpc.plugin.classifier=$GRPC_CLASSIFIER package -DskipTests

FROM eclipse-temurin:25-jre
WORKDIR /app
COPY --from=builder /build/target/delivery-service-0.1.0.jar /app/delivery-service.jar
EXPOSE 9090
ENTRYPOINT ["java", "-XX:StartFlightRecording=filename=/tmp/delivery.jfr,duration=150s,dumponexit=true,settings=profile", "-jar", "/app/delivery-service.jar"]
