# Multi-stage build. Build context is the repo root (see docker-compose build.context).

# Stage 1: build the fat jar.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace

# Copy POMs first so the dependency layer caches across source-only changes.
COPY pom.xml ./
COPY payment-service/pom.xml payment-service/
COPY ledger-service/pom.xml ledger-service/
COPY fraud-service/pom.xml fraud-service/
RUN mvn -q -pl payment-service -am dependency:go-offline

COPY payment-service/src payment-service/src
RUN mvn -q -pl payment-service -am -DskipTests package

# Stage 2: slim runtime (curl is used by the compose healthcheck).
FROM eclipse-temurin:21-jre AS runtime
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --system --uid 10001 appuser
USER appuser

COPY --from=build /workspace/payment-service/target/payment-service-*.jar app.jar

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
