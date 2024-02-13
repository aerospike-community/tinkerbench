FROM amazoncorretto:17
RUN yum -y install maven
ADD . /opt/tinkerbench
WORKDIR /opt/tinkerbench
RUN mvn clean package
ENTRYPOINT ["java", "-jar", "/opt/tinkerbench/target/tinkerBench-1.0-SNAPSHOT-jar-with-dependencies.jar"]

