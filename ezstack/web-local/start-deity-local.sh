#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASE_DIR=$(dirname $DIR)
CLEAN=
VERSION="0.1"
DEITY_DIR=${BASE_DIR}/denormalization-deity

mkdir -p ${DIR}/deploy/denormalization-deity
pushd ${DIR}/deploy/denormalization-deity

java -Dlog4j.configuration=file:${DIR}/log4j-console.xml -Dsamza.log.dir=${DIR} -jar ${DEITY_DIR}/target/ezstack-denormalization-deity-${VERSION}-SNAPSHOT.jar -config-path ${DEITY_DIR}/src/main/config/denormalization-deity-app.properties --config-factory org.apache.samza.config.factories.PropertiesConfigFactory

popd
