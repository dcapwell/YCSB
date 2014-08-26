#!/usr/bin/env bash

set -e

build() {
	mvn clean package dependency:copy-dependencies
}

package() {
	tar zcvf tachyon-ycsb.tar.gz * 
}


sync() {
	mv tachyon-ycsb.tar.gz ~/src/random/gpcloud-hadoop-bootstrap/
}

main() {
  build
  package
  sync
}

main "$@"

