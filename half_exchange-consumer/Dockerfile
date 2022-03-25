#BASED ON JDK 11 image
FROM openjdk:11

# Docker port exposed
EXPOSE 8060

# add jar to container
ADD ./consumer.jar consumer.jar

# command to launch
ENTRYPOINT ["java", "-jar", "/consumer.jar"]