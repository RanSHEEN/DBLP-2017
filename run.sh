#!/usr/bin/env bash
set -e  # 脚本遇到错误自动退出
set -x  # 打印执行的每条命令（调试用）

### === 配置 JVM 参数 ===
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

### === 检查 JAR 文件存在 ===
JAR_PATH="target/dblp-1.0-SNAPSHOT.jar"
if [ ! -f "$JAR_PATH" ]; then
  echo "[ERROR] JAR file not found: $JAR_PATH"
  exit 1
fi

### === 判断参数数量并执行 ===
if [ "$#" -eq 1 ]; then
  echo "[INFO] Running in HDT-only mode..."
  java "${JAVA_OPTS[@]}" -cp "$JAR_PATH" org.uu.nl.Main "$1"

elif [ "$#" -eq 5 ]; then
  echo "[INFO] Running in full mode with HDT + XML + DTD + years..."
  java "${JAVA_OPTS[@]}" -jar "$JAR_PATH" "$@"

else
  echo "[ERROR] Invalid arguments."
  echo "Usage:"
  echo "  ./run.sh <dblp.hdt>"
  echo "  ./run.sh <dblp.hdt> <dblp.xml> <dblp.dtd> <year1> <year2>"
  exit 2
fi
