#!/bin/sh

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASE_DIR=$(dirname $DIR)
CLEAN=
VERSION="0.1"
DENORMALIZER_DIR=${BASE_DIR}/denormalizer

#java -Dlog4j.configuration=file:${DIR}/log4j-console.xml -Dsamza.log.dir=${DIR} -jar ${DENORMALIZER_DIR}/target/ezapp-denormalizer-${VERSION}-SNAPSHOT.jar -config-path ${DENORMALIZER_DIR}/src/main/config/fanout-app-local-runner.properties --config-factory org.apache.samza.config.factories.PropertiesConfigFactory

mkdir -p ${DIR}/deploy/denormalizer
pushd ${DIR}/deploy/denormalizer

java -Dlog4j.configuration=file:${DIR}/log4j-console.xml -Dsamza.log.dir=${DIR} -jar ${DENORMALIZER_DIR}/target/ezapp-denormalizer-${VERSION}-SNAPSHOT.jar -config-path ${DENORMALIZER_DIR}/src/main/config/document-resolver-local-app.properties --config-factory org.apache.samza.config.factories.PropertiesConfigFactory

popd