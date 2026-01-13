# ================================
# TinkerBench Matrix Test Runner
# ================================

param(
    [string]$CsvPath = "./tbTests.csv",
    [int]$Retry = 0,
    [bool]$ContinueOnError = $true,
    [int]$Skip = 0,
    [string]$Only = "",
    [string]$LogDir = "./logs",
    [switch]$SummaryOnly,
    [switch]$Help
)

# -----------------------------
# HELP
# -----------------------------
if ($Help) {
    Write-Host ""
    Write-Host "TinkerBench Matrix Test Runner" -ForegroundColor Cyan
    Write-Host "----------------------------------------"
    Write-Host ""
    Write-Host "Usage:"
    Write-Host "  runTBTests.ps1 [options]"
    Write-Host ""
    Write-Host "Options:"
    Write-Host ""
    Write-Host "  --csv <path>"
    Write-Host "      Path to the CSV file containing test definitions."
    Write-Host "      Default: ./tbTests.csv"
    Write-Host ""
    Write-Host "  --retry <number>"
    Write-Host "      Number of retries per test before marking it as failed."
    Write-Host "      Default: 0"
    Write-Host ""
    Write-Host "  --continue-on-error <true|false>"
    Write-Host "      Whether to continue running tests after a failure."
    Write-Host "      true  = keep going"
    Write-Host "      false = stop on first failure"
    Write-Host "      Default: true"
    Write-Host ""
    Write-Host "  --skip <number>"
    Write-Host "      Skip the first N tests in the CSV file."
    Write-Host "      Default: 0"
    Write-Host ""
    Write-Host "  --only <list>"
    Write-Host "      Run only the specified test numbers (comma-separated)."
    Write-Host "      Example: --only 3,7,9"
    Write-Host "      Default: run all tests"
    Write-Host ""
    Write-Host "  --logdir <path>"
    Write-Host "      Directory where test logs will be written."
    Write-Host "      Default: ./logs"
    Write-Host ""
    Write-Host "  --summary-only"
    Write-Host "      Suppress per-test output and show only the final summary and coverage report."
    Write-Host ""
    Write-Host "  --help"
    Write-Host "      Display this help message and exit."
    Write-Host ""
    exit 0
}

# -----------------------------
# CONFIGURATION (from CLI)
# -----------------------------
# Convert comma-separated ONLY list → array of ints
$OnlyRunTests = @()
if ($Only -ne "") {
    $OnlyRunTests = $Only.Split(",") | ForEach-Object { [int]$_ }
}

$RetryCount = $Retry
$SkipFirstTest = $Skip

# -----------------------------
# HELPER: CONDITIONAL DETAIL OUTPUT
# -----------------------------
function Write-Detail {
    param(
        [string]$Message,
        [ConsoleColor]$Color = [ConsoleColor]::White
    )

    if (-not $SummaryOnly) {
        Write-Host $Message -ForegroundColor $Color
    }
}

# -----------------------------
# DISCOVER JAR FILE
# -----------------------------
$JarFile = Get-ChildItem -Path "target" -Filter "tinkerbench-*-jar-with-dependencies.jar" -ErrorAction SilentlyContinue |
           Sort-Object Name |
           Select-Object -Last 1

if (-not $JarFile) {
    Write-Host "Error: Could not find the JAR file in ./target (tinkerbench-*-jar-with-dependencies.jar)" -ForegroundColor Red
    exit 1
}

$JarName = $JarFile.Name
$JarPath = $JarFile.FullName

# -----------------------------
# LOAD TEST DEFINITIONS (CSV)
# -----------------------------
if (-not (Test-Path $CsvPath)) {
    Write-Host "CSV file not found: $CsvPath" -ForegroundColor Red
    exit 1
}

$Commands = Import-Csv -Path $CsvPath

# -----------------------------
# SUMMARY / COVERAGE TRACKING
# -----------------------------
$Total   = 0
$Passed  = 0
$Failed  = 0
$Skipped = 0
$Ran     = 0

$Coverage = @()  # array of PSCustomObject

# -----------------------------
# RUN A SINGLE TEST
# -----------------------------
function Run-Test {
    param(
        [int]$Index,
        [string]$ExpectedRC,
        [string]$Command,
        [string]$LogDir,
        [int]$RetryCount,
        [switch]$SummaryOnly
    )

    $LogFile = Join-Path $LogDir ("test_{0}.log" -f $Index)
    $Attempt = 0

    Write-Detail ("[{0}] START TEST #{1}" -f (Get-Date), $Index) Cyan
    Write-Detail ("Command: {0}" -f $Command)
    Write-Detail ("Expected RC: {0}" -f $ExpectedRC)
    Write-Detail ("Log: {0}" -f $LogFile)
    Write-Detail ("----------------------------------------")

    while ($Attempt -le $RetryCount) {

        Write-Detail ("Test: {0} Attempt {1}/{2}" -f $Index, ($Attempt+1), ($RetryCount+1)) Yellow
        Write-Detail ("[{0}] Test: {1} Executing:" -f (Get-Date), $Index) Yellow
        Write-Detail $Command

        # Run Java command
        # NOTE: We keep the entire command string as-is and let PowerShell pass it to java.
        # If you need more granular arg splitting later, we can refine this.
        & java $Command 2>&1 | Tee-Object -FilePath $LogFile
        $RC = $LASTEXITCODE

        Write-Detail ("[{0}] Return code: {1}" -f (Get-Date), $RC)

        if ($ExpectedRC -eq "ANY" -or $RC -eq [int]$ExpectedRC) {
            if (-not $SummaryOnly) {
                Write-Host ("[{0}] RESULT: PASS" -f (Get-Date)) -ForegroundColor Green
            }
            return @{ Status = "PASS"; RC = $RC }
        }

        $Attempt++
        if ($Attempt -le $RetryCount) {
            Write-Detail "Retrying..." Yellow
        }
    }

    if (-not $SummaryOnly) {
        Write-Host ("[{0}] RESULT: FAIL (expected {1}, got {2})" -f (Get-Date), $ExpectedRC, $RC) -ForegroundColor Red
    }
    return @{ Status = "FAIL"; RC = $RC }
}

# -----------------------------
# MAIN EXECUTION LOOP
# -----------------------------
Write-Detail "========================================" Cyan
Write-Detail " MATRIX TEST RUNNER START" Cyan
Write-Detail (" JAR FILE: {0}" -f $JarName) Cyan
Write-Detail "========================================" Cyan
Write-Detail ""

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

foreach ($Test in $Commands) {
    $Total++

    $ExpectedRC = $Test.ExpectedRC
    $Command    = $Test.Command

    # Skip logic
    if ($Total -le $SkipFirstTest) {
        Write-Detail ("Skipping test #{0} as per configuration (skip={1})." -f $Total, $SkipFirstTest) Yellow
        $Skipped++
        $Coverage += [pscustomobject]@{
            Index   = $Total
            Status  = "SKIPPED"
            Command = $Command
            RC      = "N/A"
        }
        continue
    }

    if ($OnlyRunTests.Count -gt 0 -and ($OnlyRunTests -notcontains $Total)) {
        Write-Detail ("Skipping test #{0} as it's not in ONLY_RUN_TESTS." -f $Total) Yellow
        $Skipped++
        $Coverage += [pscustomobject]@{
            Index   = $Total
            Status  = "SKIPPED"
            Command = $Command
            RC      = "N/A"
        }
        continue
    }

    # Run the test
    $Result = Run-Test -Index $Total `
                       -ExpectedRC $ExpectedRC `
                       -Command $Command `
                       -LogDir $LogDir `
                       -RetryCount $RetryCount `
                       -SummaryOnly:$SummaryOnly

    $Ran++

    $Coverage += [pscustomobject]@{
        Index   = $Total
        Status  = $Result.Status
        Command = $Command
        RC      = ("{0}/{1}" -f $ExpectedRC, $Result.RC)
    }

    if ($Result.Status -eq "PASS") { $Passed++ }
    else { $Failed++ }

    if (-not $ContinueOnError -and $Result.Status -eq "FAIL") {
        Write-Detail "Stopping due to failure (continue-on-error disabled)." Red
        break
    }

    if ($OnlyRunTests.Count -gt 0 -and $Ran -eq $OnlyRunTests.Count) {
        Write-Detail ("Reached the limit of ONLY_RUN_TESTS ({0} tests). Exiting main loop." -f $OnlyRunTests.Count) Cyan
        break
    }
}

# -----------------------------
# FINAL SUMMARY
# -----------------------------
Write-Host ""  # always show summary
Write-Host "========================================" -ForegroundColor Cyan
Write-Host " MATRIX TEST RUNNER COMPLETE" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

Write-Host ("Passed:  {0}" -f $Passed)  -ForegroundColor Green
Write-Host ("Failed:  {0}" -f $Failed)  -ForegroundColor Red
Write-Host ("Skipped: {0}" -f $Skipped) -ForegroundColor Yellow
Write-Host ("Ran:     {0}" -f $Ran)
Write-Host ("Total:   {0}" -f $Total)
Write-Host ("Logs:    {0}" -f $LogDir)
Write-Host ""

# -----------------------------
# COVERAGE REPORT
# -----------------------------
Write-Host "========== COVERAGE REPORT ==========" -ForegroundColor Blue

foreach ($Row in $Coverage) {
    switch ($Row.Status) {
        "PASS" {
            Write-Host ("[PASS] Test #{0} → {1}  → RC: {2}" -f $Row.Index, $Row.Command, $Row.RC) -ForegroundColor Green
        }
        "SKIPPED" {
            Write-Host ("[SKIP] Test #{0} → {1}  → RC: {2}" -f $Row.Index, $Row.Command, $Row.RC) -ForegroundColor Yellow
        }
        default {
            Write-Host ("[FAIL] Test #{0} → {1}  → RC: {2}" -f $Row.Index, $Row.Command, $Row.RC) -ForegroundColor Red
        }
    }
}

Write-Host "======================================" -ForegroundColor Blue

# Exit code for CI
if ($Failed -gt 0) {
    exit 1
}

exit 0
