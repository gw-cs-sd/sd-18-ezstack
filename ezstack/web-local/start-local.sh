#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASE_DIR=$(dirname $DIR)
CLEAN=
VERSION="0.1"

function print_usage_and_exit {
    cat <<EOF
    $(basename $0) - Start
    Use this script to run EZapp locally
    Options:
      -h, --help    This help :)
      -c --clean    Clear out all datastores prior to running
    Examples:
        Starting ezapp with empty data stores
            ./$(basename $0) --clean
EOF

    exit 2
}

function timestamp {
    local epoch=${1:-}

    if [[ ${epoch} == true ]] ; then
        date '+%s'
    else
        date '+%F %H:%M:%S'
    fi
}

function log_output {
    LOG_LEVEL="${1:-INFO}"
    echo "[$(timestamp)] $(basename ${0}) ${LOG_LEVEL}: ${@:2}" >&2
}

function error {
    log_output ERROR "${@}"
    exit 1
}

function warn {
    log_output WARNING "${@}"
}

function notice {
    log_output NOTICE "${@}"
}

function info {
    log_output INFO "${@}"
}

function clean {
    rm -rf ${DIR}/deploy
    rm -rf /tmp/kafka-logs /tmp/zookeeper
    ${DIR}/grid install zookeeper
    ${DIR}/grid install kafka
    ${DIR}/grid-elastic install elasticsearch
    ${DIR}/grid-elastic install kibana
}


if [[ $# -gt 0 ]]; then
    while [[ $# -gt 0 ]]; do
        case "${1}" in
            -h|--help)
                print_usage_and_exit
                ;;
            -c|--clean)
                clean
                shift 1
                ;;
        esac
    done
fi

PING=$(curl -s "localhost:8081/ping")
if [[ ${PING} == "pong" ]]; then
    error EZapp is already running!
fi

if [[ ! -d ${DIR}/deploy ]] ; then
    clean
fi

${DIR}/grid start zookeeper
${DIR}/grid start kafka
${DIR}/grid-elastic start elasticsearch
${DIR}/grid-elastic start kibana

info "Running EZapp"
java -jar ${BASE_DIR}/web/target/ezapp-web-${VERSION}-SNAPSHOT.jar server ${DIR}/config-local.yaml

${DIR}/grid stop kafka
${DIR}/grid stop zookeeper
${DIR}/grid-elastic stop elasticsearch
${DIR}/grid-elastic stop kibana