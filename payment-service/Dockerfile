# Multi-stage build. Build context is the REPO ROOT (see docker-compose build.context),
# because this module inherits from the root parent POM.

# ---- Stage 1: build the fat jar with Maven + JDK 21 ----
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace

# Copy ONLY the POMs first. Because these change rarely, Docker caches the
# dependency download below and skips it on rebuilds where only source changed.
COPY pom.xml ./
COPY payment-service/pom.xml payment-service/
COPY ledger-service/pom.xml ledger-service/

# Pre-resolve this module's dependencies (+ -am builds the parent it needs).
# Best-effort: anything missed here is fetched during package.
RUN mvn -q -pl payment-service -am dependency:go-offline

# Now copy source and build just this module. -DskipTests: image builds shouldn't
# run the Testcontainers suite; we run tests separately.
COPY payment-service/src payment-service/src
RUN mvn -q -pl payment-service -am -DskipTests package

# ---- Stage 2: slim runtime (JRE only, no Maven/JDK) ----
FROM eclipse-temurin:21-jre AS runtime
WORKDIR /app

# curl is used by the compose healthcheck to probe /actuator/health.
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Run as a non-root user (good practice; interviewers look for this).
RUN useradd --system --uid 10001 appuser
USER appuser

COPY --from=build /workspace/payment-service/target/payment-service-*.jar app.jar

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
