#!/usr/bin/env bash

build() {
  mvn clean install && mvn assembly:single
}

run() {
  # cleanup
  pushd /opt/tachyon/tachyon-0.6.0-SNAPSHOT/
  ./bin/tachyon tfs rm usertable
  popd

  # generate
  ./bin/ycsb load tachyon -P workloads/workloadd -p uri=tachyon://vm1:19998 2>&1
}

main() {
  local cmd="${1:-run}"
  $cmd
}

main "$@"

