#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# CONFIGURATION
# -----------------------------
LOG_DIR="./logs"
RETRY_COUNT=2          # Number of retries per test
CONTINUE_ON_ERROR=false # true = keep going, false = stop on first failure


# Find the jar file with dependencies
JAR_FILE=$(find target -maxdepth 1 -type f -name 'tinkerbench-*-jar-with-dependencies.jar' 2>/dev/null \
  | sort -V \
  | tail -n 1
)
if [ -z "$JAR_FILE" ]; then
    echo "Error: Could not find the JAR file"
    exit 1
fi

# Extract just the filename
JAR_NAME=$(basename "$JAR_FILE")


declare -a COMMANDS=(
  "0|java -jar ./target/$JAR_NAME AirRoutesQuery2 --duration PT1m -id IdChainSampler -import './src/test/AirRoutesQuery2-Chaining-Ids.csv' -prom"
  "0|java -jar ./target/$JAR_NAME 'g.V(%s).out('REL_DEVICE_TO_INDIVIDUAL').in('REL_DEVICE_TO_INDIVIDUAL')' -a 34.123.43.17"
  "0|java -jar ./target/$JAR_NAME --help"
  "0|java -jar ./target/$JAR_NAME TestRun --no-prometheus -d 15"
  "0|java -jar ./target/$JAR_NAME --version"
  "0|java -jar ./target/$JAR_NAME AirRoutesQuery1 -d 1m -export './src/test/idsexport.csv'"
  "0|java -jar ./target/$JAR_NAME AirRoutesQuery1 -d 1m -label city -import './src/test' -prom"
  "0|java -jar ./target/$JAR_NAME AirRoutesQuery1 -d 1m -import './src/test' -prom"
  "0|java -jar ./target/$JAR_NAME AirRoutesQuery1 -d 1m -import './src/test/idsexport.csv'"
  "0|java -jar ./target/$JAR_NAME AirRoutesQuery1 -d 1m -import './src/test/ids100.csv' -prom"
  "0|java -jar ./target/$JAR_NAME AirRoutesQuery1 -d 1m -label city -import './src/testnone' -prom"
  "0|java -jar ./target/$JAR_NAME AirRoutesQuery1 -d 1M -t evaluationTimeout=30000 -t paging=2"
  "0|java -jar ./target/$JAR_NAME AirRoutesQuery1 -d 5M -s 10 -w 20 -q 1000"
  "0|java -jar ./target/$JAR_NAME AirRoutesQuery1 -d 5M -s 10 -w 20 -q 1000 -prom"
  "0|java -jar ./target/$JAR_NAME AirRoutesQuery1 -d PT5M -wu PT2M -s 2 -prom --HdrHistFmt"
  "0|java -jar ./target/$JAR_NAME AirRoutesQuery1 -n 10.0.0.1,10.0.0.2,10.0.0.3"
  "0|java -jar ./target/$JAR_NAME AirRoutesQuery1 -d 5M -wu 2M -s 1 -w 10 -q 500 -prom"
  "0|java -jar ./target/$JAR_NAME AirRoutesQuery1 -d 15 -s 1 -w 10 -prom -debug"
  "0|java -jar ./target/$JAR_NAME AirRoutesQuery1 -d PT1M -q 100 -s 1 -w 1"
  "0|java -jar ./target/$JAR_NAME AirRoutesQuery1 -d PT1M -b junkptop=1"
  "0|java -jar ./target/$JAR_NAME AirRoutesQuery1 -d PT1M -b maxConnectionPoolSize=-1"
  "0|java -jar ./target/$JAR_NAME AirRoutesQuery1 -d PT1M -b maxConnectionPoolSize=8 -g evaluationTimeout=30000"
  "0|java -jar ./target/$JAR_NAME AirRoutesQuery1 -d PT1M -b maxConnectionPoolSize=8"
  "0|java -jar ./target/$JAR_NAME AirRoutesQuery1 -d PT1M -b maxConnectionPoolSize=abc1"
  "0|java -jar ./target/$JAR_NAME AirRoutesQuery1 -d PT1M -prom"
  "0|java -jar ./target/$JAR_NAME AirRoutesQuery1 -d PT1M -s 1 -w 10 -prom --HdrHistFmt"
  "0|java -jar ./target/$JAR_NAME AirRoutesQuery1 -d PT1M -s 1 -w 10 -prom"
  "0|java -jar ./target/$JAR_NAME AirRoutesQuery1 -d PT1M"
  "0|java -jar ./target/$JAR_NAME 'g.V(%1\$s,%-98\$s)' --duration 1m -id com.aerospike.idmanager.IdChainSampler --IdGremlinQuery 'g.with('evaluationTimeout',3000000).V().has('airport','code',within('SFO')).as('start').repeat(out('route').simplePath()).emit(not(out('route')).or().loops().is(3)).times(3).path().by(id).as('p').select('start','p').by(id).by().group().by('start').by('p').unfold().project('startId','paths').by(keys).by(values).toList()' -prom"
  "0|java -jar ./target/$JAR_NAME 'g.V(%1\$s,%2\$s)' --duration 5m -id com.aerospike.idmanager.IdChainSampler --IdGremlinQuery 'g.with('evaluationTimeout',3000000).V().has('airport','code',within('SFO')).as('start').repeat(out('route').simplePath()).emit(not(out('route')).or().loops().is(3)).times(3).path().by(id).as('p').select('start','p').by(id).by().group().by('start').by('p').unfold().project('startId','paths').by(keys).by(values).toList()' -prom"
  "0|java -jar ./target/$JAR_NAME 'g.V(%1\$s,%2\$s)' --duration 5m -id com.aerospike.idmanager.IdChainSampler -import './src/test/SFO-Parents-Export.csv' -prom"
  "0|java -jar ./target/$JAR_NAME 'g.V(%1\$s,%2\$s)' --duration PT15S -id com.aerospike.idmanager.IdChainSampler -import './src/test/SFO-B-Export.csv'"
  "0|java -jar ./target/$JAR_NAME 'g.V(%1\$s,%2\$s)' --duration PT15S -id com.aerospike.idmanager.IdChainSampler --IdGremlinQuery 'g.with('evaluationTimeout',300).V().has('airport','code',within('SFO')).as('start').repeat(out('route').simplePath()).emit(not(out('route')).or().loops().is(5)).times(5).path().by(id).as('p').select('start','p').by(id).by().group().by('start').by('p').unfold().project('startId','paths').by(keys).by(values).toList()'"
  "0|java -jar ./target/$JAR_NAME 'g.V(%1\$s,%2\$s)' --duration PT15S -id null --IdGremlinQuery 'g.with('evaluationTimeout',3000000).V().has('airport','code',within('SFO')).as('start').repeat(out('route').simplePath()).emit(not(out('route')).or().loops().is(3)).times(3).path().by(id).as('p').select('start','p').by(id).by().group().by('start').by('p').unfold().project('startId','paths').by(keys).by(values).toList()'"
  "0|java -jar ./target/$JAR_NAME 'g.V(%1\$s,%2\$s).limit(2)' --duration PT15S -id com.aerospike.idmanager.IdChainSampler --IdGremlinQuery 'g.with('evaluationTimeout',3000).V(23).path().by(id)'"
  "0|java -jar ./target/$JAR_NAME 'g.V(%1\$s).outE().inV().hasId(%1\$s)' --duration PT15S"
  "0|java -jar ./target/$JAR_NAME 'g.V(%1\$s).outE().inV().hasId(%2\$s)' --duration PT1m -id com.aerospike.idmanager.IdChainSampler -import './src/test/SFO-B-results.csv' -prom"
  "0|java -jar ./target/$JAR_NAME 'g.V(%1\$s).outE().inV().hasId(%2\$s)' --duration PT15S"
  "0|java -jar ./target/$JAR_NAME 'g.V(%1\$s).outE().inV().hasId(%5\$s)' --duration 15s -id com.aerospike.idmanager.IdChainSampler -import './src/test/SFO-B-results.csv' -prom"
  "0|java -jar ./target/$JAR_NAME 'g.V(%1\$s).out().count()' --duration PT15S"
  "0|java -jar ./target/$JAR_NAME 'g.V(%1\$s,%2\$s)' --duration PT15S -id com.aerospike.idmanager.IdChainSampler --IdGremlinQuery 'g.with('evaluationTimeout',3000000).V().has('airport','code',within('SFO')).as('start').repeat(out('route').simplePath()).emit(not(out('route')).or().loops().is(3)).times(3).path().by(id).as('p').select('start','p').by(id).by().group().by('start').by('p').unfold().project('startId','paths').by(keys).by(values).toList()' -export './src/test/SFO-Parents-Export.csv'"
  "0|java -jar ./target/$JAR_NAME 'g.V(%8\$s).outE().inV().hasId(%4\$s)' --duration PT15S -id com.aerospike.idmanager.IdChainSampler -import './src/test/SFO-B-results.csv' -export './src/test/SFO-B-Export.csv'"
  "0|java -jar ./target/$JAR_NAME 'g.V(%8\$s).outE().inV().hasId(%4\$s)' --duration PT15S -id com.aerospike.idmanager.IdChainSampler -import './src/test/SFO-B-results.csv'"
  "0|java -jar ./target/$JAR_NAME 'g.V(%s).outE().inV().hasId(%s)' --duration PT15S -id com.aerospike.idmanager.IdChainSampler -import './src/test/SFO-B-results.csv'"
  "0|java -jar ./target/$JAR_NAME 'g.V(%s).out().count().toList()' --duration 2M -prom"
  "0|java -jar ./target/$JAR_NAME 'g.V(%s).out().count().toList()' --duration PT15S"
  "0|java -jar ./target/$JAR_NAME 'g.V(%s,%s).limit(4)' --duration PT15S -wm 15s -qps 100 -incr 25 -end 200"
  "0|java -jar ./target/$JAR_NAME 'g.V(%s,%s).limit(4)' --duration PT15S -wm 15s -qps 10000"
  "0|java -jar ./target/$JAR_NAME 'g.V(%s,%s).limit(4)' --duration PT15S"
  "0|java -jar ./target/$JAR_NAME 'g.V(%s).out().limit(5).nopath().by(values('code','city').fold())' -d 15 -s 2 -prom"
  "0|java -jar ./target/$JAR_NAME TestRun --IdManager List"
  "0|java -jar ./target/$JAR_NAME 'g.V(%s).out().count().toList()' -d 1H -s 5 -w 20 -q 1500 -prom"
  "0|java -jar ./target/$JAR_NAME 'g.V(%s).out().count().toList()' -d 1M"
  "0|java -jar ./target/$JAR_NAME 'g.V(%s).out().count().toList()' -prom"
  "0|java -jar ./target/$JAR_NAME 'g.V(%s).out().limit(5).path().by(values('code','city').fold())' -d 1M -s 2 -prom -label airport,country"
  "0|java -jar ./target/$JAR_NAME 'g.V(%s).out().limit(5).path().by(values('code','city').fold())' -d 15 -s 2 -prom -label nolabel"
  "0|java -jar ./target/$JAR_NAME 'g.V(%s).out().limit(5).path().by(values('code','city').fold())' -d PT5M -wu PT2M -s 2 -prom -label airport"
  "0|java -jar ./target/$JAR_NAME 'g.V(%s).out().limit(5).path().by(values('code','city').fold())' -d PT5M -wu PT2M -s 2 -prom"
  "0|java -jar ./target/$JAR_NAME 'g.V(3).out().limit(5).path().by(values('code','city').fold()).tolist()' -d 5M -wu 2M -s 1 -w 50 -id null -prom"
  "0|java -jar ./target/$JAR_NAME 'g.V(3).out().limit(5).path().by(values('code','city').fold()).tolist()' -d 5M -wu 2M -s 1 -w 50 -sample 0 -prom"
  "0|java -jar ./target/$JAR_NAME 'g.V(%s).out().limit(5).path().by(values('code','city').fold()).tolist()' -d 5M -wu 2M -s 1 -w 50 -sample 1000 -prom"
  "0|java -jar ./target/$JAR_NAME 'g.V(%s).out('REL_DEVICE_TO_INDIVIDUAL').in('REL_DEVICE_TO_INDIVIDUAL')' -r -d 30"
  "0|java -jar ./target/$JAR_NAME 'g.V(%s).out('REL_DEVICE_TO_INDIVIDUAL').in('REL_DEVICE_TO_INDIVIDUAL')' -a 34.173.95.112"
  "0|java -jar ./target/$JAR_NAME 'g.V(%s).out('REL_DEVICE_TO_INDIVIDUAL').in('REL_DEVICE_TO_INDIVIDUAL')'"

  "0|java -jar ./target/$JAR_NAME AirRoutesQuery2"
  "0|java -jar ./target/$JAR_NAME --version"
  "0|java -Dlogback.configurationFile=./src/test/java/logback.xml -jar ./target/$JAR_NAME TestRun -d 15s -wm 15s -q 100 -incr 25 -end 200 -s 4"
  "0|java -Dlogback.configurationFile=./src/test/java/logback.xml -jar ./target/$JAR_NAME TestRun -d 1m -wm 1m -q 100 -incr 25 -end 200 -s 4 -prom"
  "0|java -Dlogback.configurationFile=./src/test/java/logback.xml -jar ./target/$JAR_NAME TestRun -s 1 -d 30 -debug"
  "0|java -Dlogback.configurationFile=./src/test/java/logback.xml -jar ./target/$JAR_NAME TestRun -d PT1M -q 100 -s 4 -prom"
  "0|java -DTBQryJar=./samples/PreDefinedQueries/target/PreDefinedQueries-1.0.jar -jar ./target/$JAR_NAME list"
  "0|java -jar ./target/$JAR_NAME list"
  "0|java -Dlogback.configurationFile=./src/test/java/logback.xml -jar ./target/$JAR_NAME 'g.V(%s).out().limit(5).path().by(values('code','city').bad.fold())' -d 1M -prom"
  "0|java -Dlogback.configurationFile=./src/test/java/logback.xml -jar ./target/$JAR_NAME 'g.V(3).out().limit(5).path().by(values('code','city').fold())' -d 1M -prom"
  "0|java -Dlogback.configurationFile=./src/test/java/logback.xml -jar ./target/$JAR_NAME 'g.V(%1\$s,%2\$s)' --duration PT15S -id disable --IdGremlinQuery 'g.with('evaluationTimeout',3000000).V().has('airport','code',within('SFO')).as('start').repeat(out('route').simplePath()).emit(not(out('route')).or().loops().is(3)).times(3).path().by(id).as('p').select('start','p').by(id).by().group().by('start').by('p').unfold().project('startId','paths').by(keys).by(values).toList()' -prom"
  "0|java -Dlogback.configurationFile=./src/test/java/logback.xml -jar ./target/$JAR_NAME 'g.V(%1\$s,%2\$s)' --duration PT15S -id list --IdGremlinQuery 'g.with('evaluationTimeout',3000000).V().has('airport','code',within('SFO')).as('start').repeat(out('route').simplePath()).emit(not(out('route')).or().loops().is(3)).times(3).path().by(id).as('p').select('start','p').by(id).by().group().by('start').by('p').unfold().project('startId','paths').by(keys).by(values).toList()' -prom"
  "0|java -Dlogback.configurationFile=./src/test/java/logback.xml -jar ./target/$JAR_NAME 'g.V(%1\$s,%2\$s)' --duration PT15S --IdGremlinQuery 'g.with('evaluationTimeout',3000000).V().has('airport','code',within('SFO')).as('start').repeat(out('route').simplePath()).emit(not(out('route')).or().loops().is(3)).times(3).path().by(id).as('p').select('start','p').by(id).by().group().by('start').by('p').unfold().project('startId','paths').by(keys).by(values).toList()' -prom"
  "0|java -Dlogback.configurationFile=./src/test/java/logback.xml -jar ./target/$JAR_NAME 'g.V(%1\$s,%2\$s)' --duration PT15S -id IdChainSampler --IdGremlinQuery 'g.with('evaluationTimeout',3000000).V().has('airport','code',within('SFO')).as('start').repeat(out('route').simplePath()).emit(not(out('route')).or().loops().is(3)).times(3).path().by(id).as('p').select('start','p').by(id).by().group().by('start').by('p').unfold().project('startId','paths').by(keys).by(values).toList()' -prom"
  "0|java -Dlogback.configurationFile=./src/test/java/logback.xml -jar ./target/$JAR_NAME 'g.V(%1\$s,%2\$s).limit(2)' --duration PT15S -id com.aerospike.idmanager.IdChainSampler --IdGremlinQuery 'g.with('evaluationTimeout',3000).V(23).path().by(id)' -prom"
  "0|java -jar ./target/$JAR_NAME 'g.V(3).out().limit(5).path().by(values('code','city').fold())' -d 5M -wu 2M -s -1 -q 500 -prom"
)


mkdir -p "$LOG_DIR"

# -----------------------------
# COLOR DEFINITIONS
# -----------------------------
RED="\033[0;31m"
GREEN="\033[0;32m"
YELLOW="\033[1;33m"
BLUE="\033[0;34m"
CYAN="\033[0;36m"
RESET="\033[0m"

# -----------------------------
# SUMMARY / COVERAGE TRACKING
# -----------------------------
TOTAL=0
PASSED=0
FAILED=0

declare -A COVERAGE_STATUS   # key = test index, value = PASS/FAIL
declare -A COVERAGE_COMMAND  # key = test index, value = command string

timestamp() {
  date +"%Y-%m-%d %H:%M:%S"
}

# -----------------------------
# EXECUTE A SINGLE TEST
# -----------------------------
run_test() {
  local entry="$1"
  local expected_rc="${entry%%|*}"
  local cmd="${entry#*|}"

  local logfile="$LOG_DIR/test_${TOTAL}.log"
  local attempt=0
  local rc=0

  COVERAGE_COMMAND[$TOTAL]="$cmd"

  echo -e "${BLUE}[$(timestamp)] START TEST #$TOTAL${RESET}"
  echo -e "${CYAN}Command:${RESET} $cmd"
  echo -e "${CYAN}Expected RC:${RESET} $expected_rc"
  echo -e "${CYAN}Log:${RESET} $logfile"
  echo "----------------------------------------"

  # Retry loop
  while (( attempt <= RETRY_COUNT )); do
    echo -e "${YELLOW}Attempt $((attempt+1))/${RETRY_COUNT}+1${RESET}"

    {
      echo "[$(timestamp)] Executing:"
      echo "$cmd"
      echo

      bash -c "$cmd"
      rc=$?

      echo
      echo "[$(timestamp)] Return code: $rc"
    } &> "$logfile"

    if [[ "$rc" -eq "$expected_rc" ]]; then
      echo -e "${GREEN}[$(timestamp)] RESULT: PASS${RESET}"
      COVERAGE_STATUS[$TOTAL]="PASS"
      ((PASSED++))
      echo
      return 0
    fi

    ((attempt++))
    echo -e "${YELLOW}Retrying...${RESET}"
  done

  # If we reach here, all retries failed
  echo -e "${RED}[$(timestamp)] RESULT: FAIL (expected $expected_rc, got $rc)${RESET}"
  COVERAGE_STATUS[$TOTAL]="FAIL"
  ((FAILED++))
  echo

  if [[ "$CONTINUE_ON_ERROR" == false ]]; then
    echo -e "${RED}Stopping due to failure (continue-on-error disabled).${RESET}"
    exit 1
  fi
}

# -----------------------------
# MAIN EXECUTION LOOP
# -----------------------------
echo -e "${CYAN}========================================${RESET}"
echo -e "${CYAN} MATRIX TEST RUNNER START ${RESET}"
echo -e "${CYAN}========================================${RESET}"
echo

for entry in "${COMMANDS[@]}"; do
  ((TOTAL++))
  run_test "$entry"
done

# -----------------------------
# FINAL SUMMARY
# -----------------------------
echo -e "${CYAN}========================================${RESET}"
echo -e "${CYAN} MATRIX TEST RUNNER COMPLETE ${RESET}"
echo -e "${CYAN}========================================${RESET}"
echo -e "${GREEN}Passed:${RESET} $PASSED"
echo -e "${RED}Failed:${RESET} $FAILED"
echo -e "${CYAN}Total:${RESET}  $TOTAL"
echo -e "${CYAN}Logs:${RESET}   $LOG_DIR"
echo

# -----------------------------
# COVERAGE REPORT
# -----------------------------
echo -e "${BLUE}========== COVERAGE REPORT ==========${RESET}"

for i in $(seq 1 "$TOTAL"); do
  status="${COVERAGE_STATUS[$i]}"
  cmd="${COVERAGE_COMMAND[$i]}"

  if [[ "$status" == "PASS" ]]; then
    echo -e "${GREEN}[PASS]${RESET} Test #$i → $cmd"
  else
    echo -e "${RED}[FAIL]${RESET} Test #$i → $cmd"
  fi
done

echo -e "${BLUE}======================================${RESET}"

# Exit code for CI
if [[ "$FAILED" -gt 0 ]]; then
  exit 1
fi

exit 0
