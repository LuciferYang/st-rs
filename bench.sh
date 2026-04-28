#!/usr/bin/env bash
# Run the JNI-overhead JMH benchmark suite.
#
# Builds the release JNI native library, compiles the bench classes
# under the `bench` Maven profile, then launches a fresh JVM with the
# right java.library.path and the test classpath so JMH's forked runs
# can find both the native lib and the bench classes.
#
# Usage:
#   ./bench.sh                              # run every benchmark
#   ./bench.sh get_hit                      # filter by regex
#   ./bench.sh -wi 5 -i 10 -f 2 get_hit     # tune iterations / forks
#
# Pass any JMH CLI flag — `./bench.sh -h` shows the full list.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
JAVA_DIR="$ROOT/java"
NATIVE_DIR="$ROOT/target/release"

echo ">> building release JNI native library..."
( cd "$ROOT" && cargo build -p st-rs-jni --release --quiet )

echo ">> compiling JMH benches under bench profile..."
mvn -B -q -f "$JAVA_DIR/pom.xml" -Pbench test-compile

echo ">> resolving test classpath..."
CP_FILE="$JAVA_DIR/target/bench-cp.txt"
mvn -B -q -f "$JAVA_DIR/pom.xml" -Pbench dependency:build-classpath \
    -DincludeScope=test \
    -Dmdep.outputFile="$CP_FILE"

CLASSPATH="$JAVA_DIR/target/test-classes:$JAVA_DIR/target/classes:$(cat "$CP_FILE")"

echo ">> launching JMH ($# args)"
exec java \
    -Djava.library.path="$NATIVE_DIR" \
    -cp "$CLASSPATH" \
    org.openjdk.jmh.Main "$@"
