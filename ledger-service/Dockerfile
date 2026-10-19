# Multi-stage build. Build context is the repo root (see docker-compose build.context).

# Stage 1: build the fat jar.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace

COPY pom.xml ./
COPY payment-service/pom.xml payment-service/
COPY ledger-service/pom.xml ledger-service/
RUN mvn -q -pl ledger-service -am dependency:go-offline

COPY ledger-service/src ledger-service/src
RUN mvn -q -pl ledger-service -am -DskipTests package

# Stage 2: slim runtime (curl is used by the compose healthcheck).
FROM eclipse-temurin:21-jre AS runtime
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --system --uid 10001 appuser
USER appuser

COPY --from=build /workspace/ledger-service/target/ledger-service-*.jar app.jar

EXPOSE 8081
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
