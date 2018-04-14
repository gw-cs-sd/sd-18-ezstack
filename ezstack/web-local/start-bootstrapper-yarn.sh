#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASE_DIR=$(dirname $DIR)
CLEAN=
VERSION="0.1"
DENORMALIZER_DIR=${BASE_DIR}/denormalizer

mkdir -p ${DIR}/deploy/bootstrapper
tar -xvf ${BASE_DIR}/denormalizer/target/ezstack-denormalizer-0.1-SNAPSHOT-dist.tar.gz -C deploy/bootstrapper

${DIR}/deploy/bootstrapper/bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://${DIR}/deploy/bootstrapper/config/bootstrapper-yarn.properties

