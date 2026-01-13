param(
    [string]$CsvPath = "./tbTests.csv",
    [int]$Retry = 0,
    [bool]$ContinueOnError = $true,
    [bool]$DisplayErrorOnly = $true,
    [int]$Skip = 0,
    [string]$Only = "",
    [string]$LogDir = "./logs",
    [switch]$Help
)

# -----------------------------
# HELP
# -----------------------------
if ($Help) {
    Write-Host ""
    Write-Host "TinkerBench Matrix Test Runner" -ForegroundColor Cyan
    Write-Host "----------------------------------------"
    Write-Host "Usage: runTBTests.ps1 [options]"
    Write-Host ""
    Write-Host "  -CsvPath <path>              Path to CSV file (default: ./tbTests.csv)"
    Write-Host "  -Retry <n>               Number of retries per test"
    Write-Host "  -ContinueOnError <t/f> Continue after failure"
    Write-Host "  -DisplayErrorOnly <t/f>     Show stderr on console only"
    Write-Host "  -Skip <n>                Skip first N tests"
    Write-Host "  -Only <list>             Run only specific tests (comma-separated)"
    Write-Host "  -LogDir <path>           Directory for logs"
    Write-Host "  -Help                    Show help"
    Write-Host ""
    exit
}

# -----------------------------
# INITIALIZATION
# -----------------------------
$OnlyRunTests = @()
if ($Only -ne "") {
    $OnlyRunTests = $Only.Split(",") | ForEach-Object { [int]$_ }
}

if (-not (Test-Path $CsvPath)) {
    Write-Host "CSV file not found: $CsvPath" -ForegroundColor Red
    exit 1
}

$JarFile = Get-ChildItem -Path "target" -Filter "tinkerbench-*-jar-with-dependencies.jar" |
           Sort-Object Name |
           Select-Object -Last 1

if (-not $JarFile) {
    Write-Host "Error: Could not find JAR file" -ForegroundColor Red
    exit 1
}

$JarName = $JarFile.Name
$JarPath = $JarFile.FullName

$Commands = Import-Csv -Path $CsvPath
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

# -----------------------------
# SUMMARY TRACKING
# -----------------------------
$Total   = 0
$Passed  = 0
$Failed  = 0
$Skipped = 0
$Ran     = 0

$Coverage = @()

# -----------------------------
# ALWAYS PRINT SUMMARY ON EXIT
# -----------------------------
$script:FinalSummaryPrinted = $false

$PrintSummary = {
    if ($script:FinalSummaryPrinted) { return }
    $script:FinalSummaryPrinted = $true

    Write-Host ""
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

    Write-Host "========== COVERAGE REPORT ==========" -ForegroundColor Blue
    foreach ($Row in $Coverage) {
        switch ($Row.Status) {
            "PASS" {
                Write-Host ("[PASS] Test #{0} → RC: {1}" -f $Row.Index, $Row.RC) -ForegroundColor Green -NoNewline
                if ($Row.NONRC) {
                    Write-Host " [Non-Zero RC]" -ForegroundColor Yellow
                }
                else {
                    Write-Host ""
                }
            }
            "SKIPPED" {
                Write-Host ("[SKIP] Test #{0}" -f $Row.Index) -ForegroundColor Yellow
            }
            default {
                Write-Host ("[FAIL] Test #{0} → RC: {1} → {2}" -f $Row.Index, $Row.RC, $Row.Command) -ForegroundColor Red
            }
        }
    }
    Write-Host "======================================" -ForegroundColor Blue
}

# Print summary on Ctrl-C
Register-EngineEvent -SourceIdentifier ConsoleCancelEventHandler -Action {
    & $PrintSummary
    $eventArgs.Cancel = $true
    exit 2
} | Out-Null


Add-Type -Namespace Win32 -Name CommandLine -MemberDefinition @"
    [DllImport("shell32.dll", SetLastError = true)]
    public static extern IntPtr CommandLineToArgvW(
        [MarshalAs(UnmanagedType.LPWStr)] string lpCmdLine,
        out int pNumArgs
    );
"@

function Split-CommandLine {
    param([string]$CommandLine)

    $argc = 0
    $ptr = [Win32.CommandLine]::CommandLineToArgvW($CommandLine, [ref]$argc)

    if ($ptr -eq [IntPtr]::Zero) {
        throw "CommandLineToArgvW failed"
    }

    $args = New-Object string[] $argc

    for ($i = 0; $i -lt $argc; $i++) {
        $p = [System.Runtime.InteropServices.Marshal]::ReadIntPtr(
            $ptr, $i * [IntPtr]::Size
        )
        $args[$i] = [System.Runtime.InteropServices.Marshal]::PtrToStringUni($p)
    }

    return $args
}

# -----------------------------
# RUN A SINGLE TEST
# -----------------------------
function Run-Test {
    param(
        [int]$Index,
        [string]$JarFileName,
        [string]$ExpectedRC,
        [string]$Command,
        [string]$LogDir,
        [int]$RetryCount,
        [bool]$DisplayErrorOnly
    )

    $LogFile = Join-Path $LogDir ("test_{0}.log" -f $Index)
    $ErrFile = Join-Path $LogDir ("test_{0}_error.log" -f $Index)

    # Replace any JAR reference with the actual discovered JAR path
    $Command = $Command -replace "tinkerbench-[^ ]*-jar-with-dependencies\.jar", $JarFileName

    # Remove outer single quotes if present
    if ($Command.StartsWith("'") -and $Command.EndsWith("'")) {
        $Command = $Command.Substring(1, $Command.Length - 2)
    }

    # Convert doubled single quotes to single quotes
    $Command = $Command -replace "''", "'"

    $Attempt = 0

    Write-Host "----------------------------------------" -ForegroundColor Cyan
    Write-Host ("[{0}] START TEST #{1}" -f (Get-Date), $Index) -ForegroundColor Cyan
    Write-Host "Command: $Command"
    Write-Host "Expected RC: $ExpectedRC"
    Write-Host "Log: $LogFile"

    # Tokenize command safely
    $cmdArray = @()
    $cmdArray = Split-CommandLine $Command

    if (Test-Path $LogFile) { Remove-Item $LogFile }
    if (Test-Path $ErrFile) { Remove-Item $ErrFile }

    while ($Attempt -le $RetryCount) {

        Write-Host ("Attempt {0}/{1}" -f ($Attempt+1), ($RetryCount+1)) -ForegroundColor Yellow

        Write-Host "Please Wait... Running..." -ForegroundColor Cyan
        if ($DisplayErrorOnly) {
            # stdout → logfile
            # stderr → console AND logfile
            $p = Start-Process "java" -ArgumentList $cmdArray `
                -NoNewWindow `
                -PassThru `
                -RedirectStandardOutput $LogFile `
                -RedirectStandardError $ErrFile

            $p.WaitForExit()
            $RC = $p.ExitCode

            if ((Test-Path $ErrFile) -and (Get-Item $ErrFile).Length -gt 0) {
                Get-Content $ErrFile | Write-Host
            }
        }
        else {
            # stdout + stderr → logfile + console
            $p = Start-Process "java" -ArgumentList $cmdArray `
                -NoNewWindow `
                -PassThru `
                -RedirectStandardOutput $LogFile `
                -RedirectStandardError "STDOUT"

            $p.WaitForExit()
            $RC = $p.ExitCode

            Get-Content $LogFile | Write-Host
        }

        # Delete empty logs
        if ((Test-Path $LogFile) -and (Get-Item $LogFile).Length -eq 0) { Remove-Item $LogFile }
        if ((Test-Path $ErrFile) -and (Get-Item $ErrFile).Length -eq 0) { Remove-Item $ErrFile }

        Write-Host ("Return code: {0}" -f $RC)

        if ($ExpectedRC -eq "ANY" -or $RC -eq [int]$ExpectedRC) {
            Write-Host "RESULT: PASS" -ForegroundColor Green
            return @{ Status="PASS"; RC=$RC }
        }

        $Attempt++
        Write-Host "Retrying..." -ForegroundColor Yellow
    }

    Write-Host ("RESULT: FAIL (expected {0}, got {1})" -f $ExpectedRC, $RC) -ForegroundColor Red
    return @{ Status="FAIL"; RC=$RC }
}

# -----------------------------
# MAIN LOOP
# -----------------------------
Write-Host "========================================" -ForegroundColor Cyan
Write-Host " MATRIX TEST RUNNER START" -ForegroundColor Cyan
Write-Host (" JAR FILE: {0}" -f $JarName) -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

$ScriptExitCode = 0

try {
    foreach ($Test in $Commands) {
        $Total++

        $ExpectedRC = $Test.ExpectedRC
        $Command    = $Test.Command

        # Skip logic
        if ($Total -le $Skip) {
            Write-Host "Skipping test #$Total" -ForegroundColor Yellow
            $Coverage += [pscustomobject]@{
                Index=$Total; Status="SKIPPED"; Command=$Command; RC="N/A"
            }
            $Skipped++
            continue
        }

        if ($OnlyRunTests.Count -gt 0 -and ($OnlyRunTests -notcontains $Total)) {
            Write-Host "Skipping test #$Total (not in ONLY list)" -ForegroundColor Yellow
            $Coverage += [pscustomobject]@{
                Index=$Total; Status="SKIPPED"; Command=$Command; RC="N/A"
            }
            $Skipped++
            continue
        }

        $Result = Run-Test -Index $Total `
                        -JarFileName $JarName `
                        -ExpectedRC $ExpectedRC `
                        -Command $Command `
                        -LogDir $LogDir `
                        -RetryCount $Retry `
                        -DisplayErrorOnly $DisplayErrorOnly

        $Ran++

        $Coverage += [pscustomobject]@{
            Index=$Total
            Status=$Result.Status
            Command=$Command
            RC=("{0}/{1}" -f $ExpectedRC, $Result.RC)
            NONRC=$Result.RC -ne 0
        }

        if ($Result.Status -eq "PASS") { $Passed++ }
        else { $Failed++ }

        if (-not $ContinueOnError -and $Result.Status -eq "FAIL") {
            Write-Host "Stopping due to failure (continue-on-error disabled)." -ForegroundColor Red
            break
        }

        if ($OnlyRunTests.Count -gt 0 -and $Ran -eq $OnlyRunTests.Count) {
            Write-Host "Reached ONLY list limit." -ForegroundColor Cyan
            break
        }
    }
}
catch {
    Write-Host "An unexpected error occurred: $_" -ForegroundColor Red
    $ScriptExitCode = 2
}
finally {
    # Set exit code to 1 if any tests failed
    if($ScriptExitCode -eq 0 && $Failed -gt 0) {
        $ScriptExitCode = 1
    }

    # Ensure summary is printed
    & $PrintSummary
}

exit $ScriptExitCode
