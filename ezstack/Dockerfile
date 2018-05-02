# FOR DEMO PURPOSES ONLY

FROM anapsix/alpine-java:8_jdk

WORKDIR /home

RUN apk add --update --no-cache curl tar maven procps bash

RUN addgroup -S ezstack && adduser -S -G ezstack ezstack

COPY . /home/

RUN HOME=/home

RUN web-local/grid install zookeeper

RUN web-local/grid install kafka

RUN web-local/grid install yarn

RUN web-local/grid-elastic install elasticsearch

RUN web-local/grid-elastic install kibana

RUN mvn clean install -DskipTests

WORKDIR /home/web-local

RUN mkdir -p /home/ezstack/.samza

RUN cp -r /root/.samza/conf /home/ezstack/.samza/

RUN chown ezstack:ezstack /home -R

USER ezstack

EXPOSE 8080

CMD mkdir -p deploy/denormalizer && \
tar -xvf ../denormalizer/target/ezstack-denormalizer-0.1-SNAPSHOT-dist.tar.gz -C deploy/denormalizer && \
./grid start zookeeper && \
./grid start kafka && \
./grid-elastic start elasticsearch && \
./grid start yarn && \
deploy/denormalizer/bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/denormalizer/config/denormalizer-yarn.properties && \
./start-deity-yarn.sh && \
java -jar ../web/target/ezapp-web-0.1-SNAPSHOT.jar server config-local.yaml