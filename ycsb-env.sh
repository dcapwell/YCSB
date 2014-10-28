export WORKLOAD=${WORKLOAD:-read-stress}
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

export FIELD_COUNT=${FIELD_COUNT:-4}
export FIELD_LENGTH=${FIELD_LENGTH:-100}
export TACHYON_HOME=${TACHYON_HOME:-/opt/tachyon/tachyon-0.6.0-SNAPSHOT}

export MASTER_HOSTNAME=${MASTER_HOSTNAME:-$(hostname -f)}
export MASTER_ADDRESS=${MASTER_ADDRESS:-"tachyon://$MASTER_HOSTNAME:19998"}

export YCSB_OPS=${YCSB_OPS:-"-P workloads/${WORKLOAD} -p fieldcount=${FIELD_COUNT} -p fieldlength=${FIELD_LENGTH} -p uri=$MASTER_ADDRESS -threads $NUM_THREADS -s"}
