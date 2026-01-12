#!/usr/bin/env bash

#set -euo pipefail
set -uo pipefail

echo "Running under: $0"
echo "BASH_VERSION=$BASH_VERSION"

# -----------------------------
# CLI ARGUMENT PARSING
# -----------------------------
LOG_DIR="./logs"
RETRY_COUNT=0
CONTINUE_ON_ERROR=true
SKIP_FIRST_TEST=0
ONLY_RUN_TESTS=()

print_help() {
  echo ""
  echo "TinkerBench Matrix Test Runner"
  echo "----------------------------------------"
  echo "Usage:"
  echo "  runTBTests.sh [options]"
  echo ""
  echo "Options:"
  echo "  --csv <path>              Path to CSV file (default: ./tbTests.csv)"
  echo "  --retry <n>               Number of retries per test (default: 0)"
  echo "  --continue-on-error <t/f> Continue after failure (default: true)"
  echo "  --skip <n>                Skip first N tests (default: 0)"
  echo "  --only <list>             Run only specific tests (comma-separated)"
  echo "  --logdir <path>           Directory for logs (default: ./logs)"
  echo "  --help                    Show this help message"
  echo ""
}

CSV_FILE="./tbTests.csv"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --csv)
      CSV_FILE="$2"
      shift 2
      ;;
    --retry)
      RETRY_COUNT="$2"
      shift 2
      ;;
    --continue-on-error)
      CONTINUE_ON_ERROR="$2"
      shift 2
      ;;
    --skip)
      SKIP_FIRST_TEST="$2"
      shift 2
      ;;
    --only)
      IFS=',' read -r -a ONLY_RUN_TESTS <<< "$2"
      shift 2
      ;;
    --logdir)
      LOG_DIR="$2"
      shift 2
      ;;
    --help)
      print_help
      exit 0
      ;;
    *)
      echo "Unknown argument: $1"
      print_help
      exit 1
      ;;
  esac
done


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

if [[ ! -f "$CSV_FILE" ]]; then
  echo "Error: CSV file not found: $CSV_FILE"
  exit 1
fi

# Read CSV into array, skipping header
mapfile -t COMMANDS < <(tail -n +2 "$CSV_FILE")

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
SKIPPED=0
RAN=0

declare -A COVERAGE_STATUS   # key = test index, value = PASS/FAIL
declare -A COVERAGE_COMMAND  # key = test index, value = command string
declare -A COVERAGE_COMMAND_RC  # key = test index, value = command's return code

timestamp() {
  date +"%Y-%m-%d %H:%M:%S"
}

print_summary() {
  echo -e "${CYAN}========================================${RESET}"
  echo -e "${CYAN} MATRIX TEST RUNNER COMPLETE ${RESET}"
  echo -e "${CYAN}========================================${RESET}"

  echo -e "${GREEN}Passed:${RESET} $PASSED"
  echo -e "${RED}Failed:${RESET} $FAILED"
  echo -e "${YELLOW}Skipped:${RESET} $SKIPPED"
  echo -e "${CYAN}Ran:${RESET}  $RAN"
  echo -e "${CYAN}Total:${RESET}  $TOTAL"
  echo -e "${CYAN}Logs:${RESET}   $LOG_DIR"
  echo

  echo -e "${BLUE}========== COVERAGE REPORT ==========${RESET}"
  for i in $(seq 1 "$TOTAL"); do
    status="${COVERAGE_STATUS[$i]}"
    cmd="${COVERAGE_COMMAND[$i]}"
    rc="${COVERAGE_COMMAND_RC[$i]}"

    if [[ "$status" == "PASS" ]]; then
      echo -e "${GREEN}[PASS]${RESET} Test #$i → $cmd  → RC: $rc"
    elif [[ "$status" == "SKIPPED" ]]; then
      echo -e "${YELLOW}[SKIP]${RESET} Test #$i → $cmd  → RC: $rc"
    else
      echo -e "${RED}[FAIL]${RESET} Test #$i → $cmd  → RC: $rc"
    fi
  done
  echo -e "${BLUE}======================================${RESET}"
}

trap print_summary EXIT

# -----------------------------
# EXECUTE A SINGLE TEST
# -----------------------------
run_test() {
  local entry="$1"

  # CSV format: ExpectedRC,Command
  local expected_rc
  local cmd

  expected_rc="$(echo "$entry" | cut -d',' -f1)"
  cmd="$(echo "$entry" | cut -d',' -f2-)"

  # Strip trailing CR (if file is CRLF)
  cmd="${cmd%$'\r'}"
  # Strip leading and trailing single quotes if present
  cmd="${cmd#\"\'}"
  cmd="${cmd%\"\'\"}"

  # If the string starts and ends with a double-quote, remove them
  if [[ "$cmd" == \"*\" ]]; then
    cmd="${cmd:1:${#cmd}-2}"
  fi
  cmd="${cmd//\'\'/\"}"

  eval "set -- $cmd"
  local evalcmd="$@"
  local -a cmd_array

  read -r -a cmd_array <<< "$evalcmd"

  local logfile="$LOG_DIR/test_${TOTAL}.log"
  local attempt=0
  local rc=0

  COVERAGE_COMMAND[$TOTAL]="$evalcmd"

  echo -e "${BLUE}======================================${RESET}"

  echo -e "${BLUE}[$(timestamp)] START TEST #$TOTAL${RESET}"
  echo -e "${CYAN}Command:${RESET} $cmd"
  echo -e "${CYAN}Expected RC:${RESET} $expected_rc"
  echo -e "${CYAN}Log:${RESET} $logfile"
  echo "----------------------------------------"

  # Retry loop
  while (( attempt <= RETRY_COUNT )); do
    echo -e "${YELLOW}Test: ${TOTAL} Attempt $((attempt+1))/$((RETRY_COUNT+1))${RESET}"

    echo -e "[$(timestamp)] ${YELLOW}Test: ${TOTAL}${RESET} Executing:"
    echo "$evalcmd"
    echo

    java "${cmd_array[@]}" 2>&1 | tee "$logfile"
    #java $evalcmd 2>&1 | tee "$logfile"
    rc=$?

    #${PIPESTATUS[0]}
    echo
    echo "[$(timestamp)] Return code: $rc"
    COVERAGE_COMMAND_RC[$TOTAL]="${expected_rc}/${rc}"

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
    return 2
  fi
  return 1
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
    COVERAGE_STATUS[$TOTAL]="SKIPPED"
    COVERAGE_COMMAND[$TOTAL]="${entry#*|}"
    COVERAGE_COMMAND_RC[$TOTAL]="N/A"
    ((SKIPPED++))
    continue
  fi
  if [[ ${#ONLY_RUN_TESTS[@]} -gt 0 ]]; then
    if [[ ! " ${ONLY_RUN_TESTS[*]} " =~ " ${TOTAL} " ]]; then
      echo -e "${YELLOW}Skipping test #$TOTAL as it's not in ONLY_RUN_TESTS.${RESET}"
      COVERAGE_STATUS[$TOTAL]="SKIPPED"
      COVERAGE_COMMAND[$TOTAL]="${entry#*|}"
      COVERAGE_COMMAND_RC[$TOTAL]="N/A"
      ((SKIPPED++))
      continue
    fi
  fi
  run_test "$entry"
  rc=$?
  ((RAN++))
  if [[ "$rc" -eq 2 ]]; then
    echo -e "${RED}Exiting main loop due to test failure and continue-on-error disabled.${RESET}"
    break
  fi
  if [[ "$RAN" -eq "${#ONLY_RUN_TESTS[@]}" ]]; then
    echo -e "${CYAN}Reached the limit of ONLY_RUN_TESTS (${#ONLY_RUN_TESTS[@]} tests). Exiting main loop.${RESET}"
    break
  fi
done

# Exit code for CI
if [[ "$FAILED" -gt 0 ]]; then
  exit 1
fi

exit 0
