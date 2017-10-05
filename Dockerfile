FROM java:8-alpine
MAINTAINER Your Name <you@example.com>

ADD target/uberjar/hashdb.jar /hashdb/app.jar

EXPOSE 3000

CMD ["java", "-jar", "/hashdb/app.jar"]
