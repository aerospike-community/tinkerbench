#!/usr/bin/env bash

#set -euo pipefail
set -uo pipefail

echo "Running under: $0"
echo "BASH_VERSION=$BASH_VERSION"

# -----------------------------
# CONFIGURATION
# -----------------------------
LOG_DIR="./logs"
RETRY_COUNT=0          # Number of retries per test
CONTINUE_ON_ERROR=false # true = keep going, false = stop on first failure
SKIP_FIRST_TEST=0  # Number of Test to skip at start
declare -a ONLY_RUN_TESTS=()  # e.g., (3 5 7) to run only tests 3, 5, and 7

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
  "0|java -jar ./$JAR_FILE AirRoutesQuery2 --duration PT1m -id IdChainSampler -import './src/test/AirRoutesQuery2-Chaining-Ids.csv' -prom -background"
  "0|java -jar ./$JAR_FILE 'g.V(%s).out('REL_DEVICE_TO_INDIVIDUAL').in('REL_DEVICE_TO_INDIVIDUAL')' -a 34.123.43.17 -background"
  "0|java -jar ./$JAR_FILE --help"
  "0|java -jar ./$JAR_FILE TestRun --no-prometheus -d 15 -background"
  "0|java -jar ./$JAR_FILE --version"
  "0|java -jar ./$JAR_FILE AirRoutesQuery1 -d 1m -export './src/test/idsexport.csv' -background"
  "0|java -jar ./$JAR_FILE AirRoutesQuery1 -d 1m -label city -import './src/test/ids1*.csv' -prom -background"
  "0|java -jar ./$JAR_FILE AirRoutesQuery1 -d 1m -import './src/test/ids1*.csv' -prom -background"
  "0|java -jar ./$JAR_FILE AirRoutesQuery1 -d 1m -import './src/test/idsexport.csv' -background"
  "0|java -jar ./$JAR_FILE AirRoutesQuery1 -d 1m -import './src/test/ids100.csv' -prom -background"
  "0|java -jar ./$JAR_FILE AirRoutesQuery1 -d 1m -label city -import './src/testnone' -prom -background"
  "0|java -jar ./$JAR_FILE AirRoutesQuery1 -d 1M -t evaluationTimeout=30000 -t paging=2 -background"
  "0|java -jar ./$JAR_FILE AirRoutesQuery1 -d 5M -s 10 -w 20 -q 1000 -background"
  "0|java -jar ./$JAR_FILE AirRoutesQuery1 -d 5M -s 10 -w 20 -q 1000 -prom -background"
  "0|java -jar ./$JAR_FILE AirRoutesQuery1 -d PT5M -wu PT2M -s 2 -prom --HdrHistFmt -background"
  "0|java -jar ./$JAR_FILE AirRoutesQuery1 -n 10.0.0.1,10.0.0.2,10.0.0.3 -background"
  "0|java -jar ./$JAR_FILE AirRoutesQuery1 -d 5M -wu 2M -s 1 -w 10 -q 500 -prom -background"
  "0|java -jar ./$JAR_FILE AirRoutesQuery1 -d 15 -s 1 -w 10 -prom -debug -background"
  "0|java -jar ./$JAR_FILE AirRoutesQuery1 -d PT1M -q 100 -s 1 -w 1 -background"
  "0|java -jar ./$JAR_FILE AirRoutesQuery1 -d PT1M -b junkptop=1 -background"
  "0|java -jar ./$JAR_FILE AirRoutesQuery1 -d PT1M -b maxConnectionPoolSize=-1 -background"
  "0|java -jar ./$JAR_FILE AirRoutesQuery1 -d PT1M -b maxConnectionPoolSize=8 -g evaluationTimeout=30000 -background"
  "0|java -jar ./$JAR_FILE AirRoutesQuery1 -d PT1M -b maxConnectionPoolSize=8 -background"
  "0|java -jar ./$JAR_FILE AirRoutesQuery1 -d PT1M -b maxConnectionPoolSize=abc1 -background"
  "0|java -jar ./$JAR_FILE AirRoutesQuery1 -d PT1M -prom -background"
  "0|java -jar ./$JAR_FILE AirRoutesQuery1 -d PT1M -s 1 -w 10 -prom --HdrHistFmt -background"
  "0|java -jar ./$JAR_FILE AirRoutesQuery1 -d PT1M -s 1 -w 10 -prom -background"
  "0|java -jar ./$JAR_FILE AirRoutesQuery1 -d PT1M -background"
  "0|java -jar ./$JAR_FILE 'g.V(%1\$s,%-98\$s)' --duration 1m -id com.aerospike.idmanager.IdChainSampler --IdGremlinQuery 'g.with('evaluationTimeout',3000000).V().has('airport','code',within('SFO')).as('start').repeat(out('route').simplePath()).emit(not(out('route')).or().loops().is(3)).times(3).path().by(id).as('p').select('start','p').by(id).by().group().by('start').by('p').unfold().project('startId','paths').by(keys).by(values).toList()' -prom -background"
  "0|java -jar ./$JAR_FILE 'g.V(%1\$s,%2\$s)' --duration 5m -id com.aerospike.idmanager.IdChainSampler --IdGremlinQuery 'g.with('evaluationTimeout',3000000).V().has('airport','code',within('SFO')).as('start').repeat(out('route').simplePath()).emit(not(out('route')).or().loops().is(3)).times(3).path().by(id).as('p').select('start','p').by(id).by().group().by('start').by('p').unfold().project('startId','paths').by(keys).by(values).toList()' -prom -background"
  "0|java -jar ./$JAR_FILE 'g.V(%1\$s,%2\$s)' --duration 5m -id com.aerospike.idmanager.IdChainSampler -import './src/test/SFO-Parents-Export.csv' -prom -background"
  "0|java -jar ./$JAR_FILE 'g.V(%1\$s,%2\$s)' --duration PT15S -id com.aerospike.idmanager.IdChainSampler -import './src/test/SFO-B-Export.csv' -background"
  "0|java -jar ./$JAR_FILE 'g.V(%1\$s,%2\$s)' --duration PT15S -id com.aerospike.idmanager.IdChainSampler --IdGremlinQuery 'g.with('evaluationTimeout',300).V().has('airport','code',within('SFO')).as('start').repeat(out('route').simplePath()).emit(not(out('route')).or().loops().is(5)).times(5).path().by(id).as('p').select('start','p').by(id).by().group().by('start').by('p').unfold().project('startId','paths').by(keys).by(values).toList()' -background"
  "0|java -jar ./$JAR_FILE 'g.V(%1\$s,%2\$s)' --duration PT15S -id null --IdGremlinQuery 'g.with('evaluationTimeout',3000000).V().has('airport','code',within('SFO')).as('start').repeat(out('route').simplePath()).emit(not(out('route')).or().loops().is(3)).times(3).path().by(id).as('p').select('start','p').by(id).by().group().by('start').by('p').unfold().project('startId','paths').by(keys).by(values).toList()' -background"
  "0|java -jar ./$JAR_FILE 'g.V(%1\$s,%2\$s).limit(2)' --duration PT15S -id com.aerospike.idmanager.IdChainSampler --IdGremlinQuery 'g.with('evaluationTimeout',3000).V(23).path().by(id)' -background"
  "0|java -jar ./$JAR_FILE 'g.V(%1\$s).outE().inV().hasId(%1\$s)' --duration PT15S -background"
  "0|java -jar ./$JAR_FILE 'g.V(%1\$s).outE().inV().hasId(%2\$s)' --duration PT1m -id com.aerospike.idmanager.IdChainSampler -import './src/test/SFO-B-results.csv' -prom -background"
  "0|java -jar ./$JAR_FILE 'g.V(%1\$s).outE().inV().hasId(%2\$s)' --duration PT15S -background"
  "0|java -jar ./$JAR_FILE 'g.V(%1\$s).outE().inV().hasId(%5\$s)' --duration 15s -id com.aerospike.idmanager.IdChainSampler -import './src/test/SFO-B-results.csv' -prom -background"
  "0|java -jar ./$JAR_FILE 'g.V(%1\$s).out().count()' --duration PT15S -background"
  "0|java -jar ./$JAR_FILE 'g.V(%1\$s,%2\$s)' --duration PT15S -id com.aerospike.idmanager.IdChainSampler --IdGremlinQuery 'g.with('evaluationTimeout',3000000).V().has('airport','code',within('SFO')).as('start').repeat(out('route').simplePath()).emit(not(out('route')).or().loops().is(3)).times(3).path().by(id).as('p').select('start','p').by(id).by().group().by('start').by('p').unfold().project('startId','paths').by(keys).by(values).toList()' -export './src/test/SFO-Parents-Export.csv' -background"
  "0|java -jar ./$JAR_FILE 'g.V(%8\$s).outE().inV().hasId(%4\$s)' --duration PT15S -id com.aerospike.idmanager.IdChainSampler -import './src/test/SFO-B-results.csv' -export './src/test/SFO-B-Export.csv' -background"
  "0|java -jar ./$JAR_FILE 'g.V(%8\$s).outE().inV().hasId(%4\$s)' --duration PT15S -id com.aerospike.idmanager.IdChainSampler -import './src/test/SFO-B-results.csv' -background"
  "0|java -jar ./$JAR_FILE 'g.V(%s).outE().inV().hasId(%s)' --duration PT15S -id com.aerospike.idmanager.IdChainSampler -import './src/test/SFO-B-results.csv' -background"
  "0|java -jar ./$JAR_FILE 'g.V(%s).out().count().toList()' --duration 2M -prom -background"
  "0|java -jar ./$JAR_FILE 'g.V(%s).out().count().toList()' --duration PT15S -background"
  "0|java -jar ./$JAR_FILE 'g.V(%s,%s).limit(4)' --duration PT15S -wm 15s -qps 100 -incr 25 -end 200 -background"
  "0|java -jar ./$JAR_FILE 'g.V(%s,%s).limit(4)' --duration PT15S -wm 15s -qps 10000 -background"
  "0|java -jar ./$JAR_FILE 'g.V(%s,%s).limit(4)' --duration PT15S -background"
  "0|java -jar ./$JAR_FILE 'g.V(%s).out().limit(5).nopath().by(values('code','city').fold())' -d 15 -s 2 -prom -background"
  "0|java -jar ./$JAR_FILE TestRun --IdManager List -background"
  "0|java -jar ./$JAR_FILE 'g.V(%s).out().count().toList()' -d 1H -s 5 -w 20 -q 1500 -prom -background"
  "0|java -jar ./$JAR_FILE 'g.V(%s).out().count().toList()' -d 1M -background"
  "0|java -jar ./$JAR_FILE 'g.V(%s).out().count().toList()' -prom -background"
  "0|java -jar ./$JAR_FILE 'g.V(%s).out().limit(5).path().by(values('code','city').fold())' -d 1M -s 2 -prom -label airport,country -background"
  "0|java -jar ./$JAR_FILE 'g.V(%s).out().limit(5).path().by(values('code','city').fold())' -d 15 -s 2 -prom -label nolabel -background"
  "0|java -jar ./$JAR_FILE 'g.V(%s).out().limit(5).path().by(values('code','city').fold())' -d PT5M -wu PT2M -s 2 -prom -label airport -background"
  "0|java -jar ./$JAR_FILE 'g.V(%s).out().limit(5).path().by(values('code','city').fold())' -d PT5M -wu PT2M -s 2 -prom -background"
  "0|java -jar ./$JAR_FILE 'g.V(3).out().limit(5).path().by(values('code','city').fold()).tolist()' -d 5M -wu 2M -s 1 -w 50 -id null -prom -background"
  "0|java -jar ./$JAR_FILE 'g.V(3).out().limit(5).path().by(values('code','city').fold()).tolist()' -d 5M -wu 2M -s 1 -w 50 -sample 0 -prom -background"
  "0|java -jar ./$JAR_FILE 'g.V(%s).out().limit(5).path().by(values('code','city').fold()).tolist()' -d 5M -wu 2M -s 1 -w 50 -sample 1000 -prom -background"
  "0|java -jar ./$JAR_FILE 'g.V(%s).out('REL_DEVICE_TO_INDIVIDUAL').in('REL_DEVICE_TO_INDIVIDUAL')' -r -d 30 -background"
  "0|java -jar ./$JAR_FILE 'g.V(%s).out('REL_DEVICE_TO_INDIVIDUAL').in('REL_DEVICE_TO_INDIVIDUAL')' -a 34.173.95.112 -background"
  "0|java -jar ./$JAR_FILE 'g.V(%s).out('REL_DEVICE_TO_INDIVIDUAL').in('REL_DEVICE_TO_INDIVIDUAL')' -background"

  "0|java -jar ./$JAR_FILE AirRoutesQuery2 -background"
  "0|java -jar ./$JAR_FILE --version"
  "0|java -Dlogback.configurationFile=./src/test/java/logback.xml -jar ./$JAR_FILE TestRun -d 15s -wm 15s -q 100 -incr 25 -end 200 -s 4 -background"
  "0|java -Dlogback.configurationFile=./src/test/java/logback.xml -jar ./$JAR_FILE TestRun -d 1m -wm 1m -q 100 -incr 25 -end 200 -s 4 -prom -background"
  "0|java -Dlogback.configurationFile=./src/test/java/logback.xml -jar ./$JAR_FILE TestRun -s 1 -d 30 -debug -background"
  "0|java -Dlogback.configurationFile=./src/test/java/logback.xml -jar ./$JAR_FILE TestRun -d PT1M -q 100 -s 4 -prom -background"
  "0|java -DTBQryJar=./samples/PreDefinedQueries/target/PreDefinedQueries-1.0.jar -jar ./$JAR_FILE list"
  "0|java -jar ./$JAR_FILE list"
  "0|java -Dlogback.configurationFile=./src/test/java/logback.xml -jar ./$JAR_FILE 'g.V(%s).out().limit(5).path().by(values('code','city').bad.fold())' -d 1M -prom -background"
  "0|java -Dlogback.configurationFile=./src/test/java/logback.xml -jar ./$JAR_FILE 'g.V(3).out().limit(5).path().by(values('code','city').fold())' -d 1M -prom -background"
  "0|java -Dlogback.configurationFile=./src/test/java/logback.xml -jar ./$JAR_FILE 'g.V(%1\$s,%2\$s)' --duration PT15S -id disable --IdGremlinQuery 'g.with('evaluationTimeout',3000000).V().has('airport','code',within('SFO')).as('start').repeat(out('route').simplePath()).emit(not(out('route')).or().loops().is(3)).times(3).path().by(id).as('p').select('start','p').by(id).by().group().by('start').by('p').unfold().project('startId','paths').by(keys).by(values).toList()' -prom -background"
  "0|java -Dlogback.configurationFile=./src/test/java/logback.xml -jar ./$JAR_FILE 'g.V(%1\$s,%2\$s)' --duration PT15S -id list --IdGremlinQuery 'g.with('evaluationTimeout',3000000).V().has('airport','code',within('SFO')).as('start').repeat(out('route').simplePath()).emit(not(out('route')).or().loops().is(3)).times(3).path().by(id).as('p').select('start','p').by(id).by().group().by('start').by('p').unfold().project('startId','paths').by(keys).by(values).toList()' -prom -background"
  "0|java -Dlogback.configurationFile=./src/test/java/logback.xml -jar ./$JAR_FILE 'g.V(%1\$s,%2\$s)' --duration PT15S --IdGremlinQuery 'g.with('evaluationTimeout',3000000).V().has('airport','code',within('SFO')).as('start').repeat(out('route').simplePath()).emit(not(out('route')).or().loops().is(3)).times(3).path().by(id).as('p').select('start','p').by(id).by().group().by('start').by('p').unfold().project('startId','paths').by(keys).by(values).toList()' -prom -background"
  "0|java -Dlogback.configurationFile=./src/test/java/logback.xml -jar ./$JAR_FILE 'g.V(%1\$s,%2\$s)' --duration PT15S -id IdChainSampler --IdGremlinQuery 'g.with('evaluationTimeout',3000000).V().has('airport','code',within('SFO')).as('start').repeat(out('route').simplePath()).emit(not(out('route')).or().loops().is(3)).times(3).path().by(id).as('p').select('start','p').by(id).by().group().by('start').by('p').unfold().project('startId','paths').by(keys).by(values).toList()' -prom -background"
  "0|java -Dlogback.configurationFile=./src/test/java/logback.xml -jar ./$JAR_FILE 'g.V(%1\$s,%2\$s).limit(2)' --duration PT15S -id com.aerospike.idmanager.IdChainSampler --IdGremlinQuery 'g.with('evaluationTimeout',3000).V(23).path().by(id)' -prom -background"
  "0|java -jar ./$JAR_FILE 'g.V(3).out().limit(5).path().by(values('code','city').fold())' -d 5M -wu 2M -s -1 -q 500 -prom -background"
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
RAN=0

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
    } 2>&1 | tee "$logfile"

    rc=${PIPESTATUS[0]}

    if [[ "$expected_rc" == "ANY" ]]; then
      echo -e "${GREEN}[$(timestamp)] RESULT: PASS (any return code accepted)${RESET}"
      COVERAGE_STATUS[$TOTAL]="PASS"
      ((PASSED++))
      echo
      return 0
    fi
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
echo -e "${CYAN} JAR FILE ${JAR_NAME} ${RESET}"
echo -e "${CYAN}========================================${RESET}"
echo

for entry in "${COMMANDS[@]}"; do
  ((TOTAL++))
  if (( TOTAL <= SKIP_FIRST_TEST )); then
    echo -e "${YELLOW}Skipping test #$TOTAL as per configuration.${RESET}"
    continue
  fi
  if [[ ${#ONLY_RUN_TESTS[@]} -gt 0 ]]; then
    if [[ ! " ${ONLY_RUN_TESTS[*]} " =~ " ${TOTAL} " ]]; then
      echo -e "${YELLOW}Skipping test #$TOTAL as it's not in ONLY_RUN_TESTS.${RESET}"
      continue
    fi
  fi
  run_test "$entry"
  ((RAN++))
  if [[ "$RAN" -eq "${#ONLY_RUN_TESTS[@]}" ]]; then
    echo -e "${CYAN}Reached the limit of ONLY_RUN_TESTS (${#ONLY_RUN_TESTS[@]} tests). Exiting main loop.${RESET}"
    break
  fi
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
