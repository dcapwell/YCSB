#!/usr/bin/env bash

export WORKLOAD=${WORKLOAD:-read-stress}
export NUM_THREADS=${NUM_THREADS:-$(nproc)}

export YCSB_OPS="-P workloads/${WORKLOAD} -p uri=tachyon://vm1:19998 -threads $NUM_THREADS -s"

build() {
  mvn clean install && mvn assembly:single
}

load() {
  # cleanup
  pushd /opt/tachyon/tachyon-0.6.0-SNAPSHOT/
  ./bin/tachyon tfs rm usertable
  popd

  # generate
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

