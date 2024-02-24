FROM marcosisocram/graalvm-nik-maven:latest AS builder

LABEL authors="marcosisocram@gmail.com"

COPY pom.xml pom.xml
COPY src src

RUN mvn native:compile

FROM debian:stable-slim

WORKDIR /app
COPY --from=builder /opt/app/target/app /app/app
EXPOSE 8080

ENTRYPOINT ["/app/app"]