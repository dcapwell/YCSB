#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

. $bin/ycsb-env.sh

build() {
  mvn clean install && mvn assembly:single
}

clean() {
  pushd $TACHYON_HOME
  ./bin/tachyon tfs rm /usertable
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

