FROM openjdk:11-jre-slim
COPY target/transfer-2.1.0.jar /opt/transfer.jar
EXPOSE 8080
CMD ["java", "-jar", "/opt/transfer.jar"]
