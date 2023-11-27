FROM openjdk:17
LABEL maintainer="Roberto Pozzi <r.robipozzi@gmail.com>"
LABEL version="1.0"
LABEL description="Spring Boot Kafka Streams temperatures"
COPY target/robipozzi-kafkastreams-temperatures-0.0.1-SNAPSHOT.jar app.jar
ENTRYPOINT ["java", "-jar", "/app.jar"]