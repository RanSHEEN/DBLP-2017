#!/usr/bin/env bash
set -e  # Script auto-exits when it encounters an error
set -x  # Print each command executed (for debugging)

### === Configuring JVM parameters ===
JAVA_OPTS=(
  -Djdk.xml.totalEntitySizeLimit=0
  -Djdk.xml.entityExpansionLimit=0
  -Djdk.xml.maxGeneralEntitySizeLimit=0
  -Xms8g
  -Xmx8g
  -XX:+UseG1GC
  -XX:+UseContainerSupport
)

echo "Using JAVA_OPTS: ${JAVA_OPTS[@]}"

### === Checking for JAR file presence ===
JAR_PATH="target/dblp-1.0-SNAPSHOT.jar"
if [ ! -f "$JAR_PATH" ]; then
  echo "[ERROR] JAR file not found: $JAR_PATH"
  exit 1
fi

### === Judge the number of arguments and execute ===
if [ "$#" -eq 1 ]; then
  echo "[INFO] Running in HDT-only mode..."
  java "${JAVA_OPTS[@]}" -cp "$JAR_PATH" org.uu.nl.Main "$1"

elif [ "$#" -eq 6 ]; then
  echo "[INFO] Running in full mode with HDT + XML + DTD + years + noise%..."
  java "${JAVA_OPTS[@]}" -jar "$JAR_PATH" "$@"

else
  echo "[ERROR] Invalid arguments."
  echo "Usage:"
  echo "  ./run.sh <dblp.hdt>"
  echo "  ./run.sh <dblp.hdt> <dblp.xml> <dblp.dtd> <year1> <year2> <noisePct>"
  exit 2
fi
