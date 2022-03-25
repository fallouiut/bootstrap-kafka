#BASED ON JDK 11 image
FROM openjdk:11

# Docker port exposed
EXPOSE 8070

# add jar to container
ADD ./desc-producer.jar desc-producer.jar

# command to launch
ENTRYPOINT ["java", "-jar", "/desc-producer.jar"]