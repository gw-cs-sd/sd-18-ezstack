#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASE_DIR=$(dirname $DIR)
CLEAN=
VERSION="0.1"
DENORMALIZER_DIR=${BASE_DIR}/denormalizer

mkdir -p ${DIR}/deploy/denormalization-deity
tar -xvf ${BASE_DIR}/denormalization-deity/target/ezstack-denormalization-deity-0.1-SNAPSHOT-dist.tar.gz -C deploy/denormalization-deity

${DIR}/deploy/denormalization-deity/bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://${DIR}/deploy/denormalization-deity/config/denormalization-deity-yarn.properties

