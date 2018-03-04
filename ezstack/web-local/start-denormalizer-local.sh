#!/bin/sh

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASE_DIR=$(dirname $DIR)
CLEAN=
VERSION="0.1"
DENORMALIZER_DIR=${BASE_DIR}/denormalizer

#java -Dlog4j.configuration=file:${DIR}/log4j-console.xml -Dsamza.log.dir=${DIR} -jar ${DENORMALIZER_DIR}/target/ezapp-denormalizer-${VERSION}-SNAPSHOT.jar -config-path ${DENORMALIZER_DIR}/src/main/config/fanout-app-local-runner.properties --config-factory org.apache.samza.config.factories.PropertiesConfigFactory

mkdir -p ${DIR}/deploy/denormalizer
tar -xvf ${BASE_DIR}/denormalizer/target/ezstack-denormalizer-0.1-SNAPSHOT-dist.tar.gz -C deploy/denormalizer

${DIR}/deploy/denormalizer/bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://${DIR}/deploy/denormalizer/config/denormalizer-yarn.properties
#java -Dlog4j.configuration=file:${DIR}/log4j-console.xml -Dsamza.log.dir=${DIR} -jar ${DENORMALIZER_DIR}/target/ezapp-denormalizer-${VERSION}-SNAPSHOT.jar -config-path ${DENORMALIZER_DIR}/src/main/config/document-resolver-local-app.properties --config-factory org.apache.samza.config.factories.PropertiesConfigFactory

