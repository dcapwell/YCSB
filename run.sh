#!/usr/bin/env bash

set -e

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

. $bin/ycsb-env.sh

build() {
  mvn clean install && mvn assembly:single
}

clean() {
  $TACHYON_HOME/bin/tachyon tfs rm /usertable
}

load() {
  ./bin/ycsb load tachyon $LOAD_OPS 1> load.log
}

run() {
  ./bin/ycsb run tachyon $RUN_OPS 1> run.log
}

main() {
  local cmd="${1:-run}"
  $cmd
}

main "$@"

