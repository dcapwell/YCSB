#!/usr/bin/env bash

export WORKLOAD=${WORKLOAD:-read-stress}
#export NUM_THREADS=${NUM_THREADS:-$(nproc)}
export NUM_THREADS=${NUM_THREADS:-$(sysctl hw.ncpu | awk '{print $2}')}
export FIELD_COUNT=4
export FIELD_LENGTH=100
export TACHYON_HOME=/Users/dcapwell/src/github/tachyon-dcapwell/assembly/target/tachyon-0.6.0-SNAPSHOT/tachyon-0.6.0-SNAPSHOT

#export YCSB_OPS="-P workloads/${WORKLOAD} -p fieldcount=${FIELD_COUNT} -p fieldlength=${FIELD_LENGTH} -p uri=tachyon://localhost:19998 -threads $NUM_THREADS -s"
export YCSB_OPS="-P workloads/${WORKLOAD} -p fieldcount=${FIELD_COUNT} -p fieldlength=${FIELD_LENGTH} -p uri=tachyon://sjc-w11.dh.greenplum.com:19998 -threads $NUM_THREADS -s"

build() {
  mvn clean install && mvn assembly:single
}

clean() {
  pushd $TACHYON_HOME
  ./bin/tachyon tfs rm usertable
  popd
}

load() {
  ./bin/ycsb load tachyon $YCSB_OPS 1> load.log 2> load.err
}

run() {
  ./bin/ycsb run tachyon $YCSB_OPS 1> run.log 2> run.err
}

main() {
  local cmd="${1:-run}"
  $cmd
}

main "$@"

