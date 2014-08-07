#!/usr/bin/env bash

build() {
  mvn clean install && mvn assembly:single
}

run() {
  ./bin/ycsb load tachyon -P workloads/workloadd -p uri=tachyon://master:19998
}

main() {
  local cmd="${1:-run}"
  $cmd
}

main "$@"

