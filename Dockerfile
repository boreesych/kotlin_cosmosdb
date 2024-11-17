FROM openjdk:22-jdk-slim

WORKDIR /app

COPY build/libs/hello_app.jar /app/hello_app.jar

ENTRYPOINT ["java", "-jar", "/app/hello_app.jar"]
