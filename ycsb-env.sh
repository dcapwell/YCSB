export WORKLOAD=${WORKLOAD:-workloadc}
case "`uname`" in
  Linux|FreeBSD|SunOS)
    export NUM_THREADS=${NUM_THREADS:-$(nproc)}
  ;;
  Darwin)
    export NUM_THREADS=${NUM_THREADS:-$(sysctl hw.ncpu | awk '{print $2}')}
  ;;
  *)
    export NUM_THREADS=${NUM_THREADS:-2}
  ;;
esac

export FIELD_COUNT=${FIELD_COUNT:-10}
export FIELD_LENGTH=${FIELD_LENGTH:-1000}
export TACHYON_HOME=${TACHYON_HOME:-/opt/tachyon/tachyon-0.6.0-SNAPSHOT}
export OPERATION_COUNT=${OPERATION_COUNT:-10000}
export RECORD_COUNT=${RECORD_COUNT:-10000}

export MASTER_HOSTNAME=${MASTER_HOSTNAME:-$(hostname -f)}
export MASTER_ADDRESS=${MASTER_ADDRESS:-"tachyon://$MASTER_HOSTNAME:19998"}

export LOAD_OPS=${LOAD_OPS:-"-P workloads/${WORKLOAD} -p recordcount=${RECORD_COUNT} -p fieldcount=${FIELD_COUNT} -p fieldlength=${FIELD_LENGTH} -p uri=$MASTER_ADDRESS -threads $NUM_THREADS -s"}
export RUN_OPS=${RUN_OPS:-"-P workloads/${WORKLOAD} -p operationcount=${OPERATION_COUNT} -p uri=$MASTER_ADDRESS -threads $NUM_THREADS -s"}
