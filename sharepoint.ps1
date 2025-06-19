<#
.SYNOPSIS
A PowerShell helper script for managing SharePoint backups.
.DESCRIPTION
This script provides a command-line interface to set up the environment, run backups, monitor progress, and perform other management tasks for the SharePoint backup tool.
It handles Python virtual environment creation, dependency installation, and execution of the underlying Python scripts.
.PARAMETER Action
The main action to perform. Valid options are: 'setup', 'run', 'monitor', 'stats', 'clean', 'help'. Defaults to 'help'.
.PARAMETER Debug
Switch to enable debug mode, which provides more verbose output from the backup script.
.EXAMPLE
.\sharepoint.ps1 -Action setup
Sets up the Python virtual environment and installs required dependencies.
.EXAMPLE
.\sharepoint.ps1 -Action run
Runs the SharePoint backup process.
.EXAMPLE
.\sharepoint.ps1 -Action run -Debug
Runs the SharePoint backup process in debug mode.
.EXAMPLE
.\sharepoint.ps1 -Action monitor
Monitors the backup progress.
.EXAMPLE
.\sharepoint.ps1 -Action stats
Displays backup statistics.
.EXAMPLE
.\sharepoint.ps1 -Action clean
Removes the local database and log files.
.EXAMPLE
.\sharepoint.ps1 -Action help
Displays the help message.
#>
param(
    [Parameter(Mandatory=$false, Position=0)]
    [ValidateSet('setup', 'run', 'monitor', 'stats', 'clean', 'help')]
    [string]$Action = 'help',

    [Parameter(Mandatory=$false)]
    [switch]$DebugMode
)

# --- Script Configuration ---
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$VenvPath = Join-Path $ScriptDir ".venv"
$PythonExecutable = Join-Path $VenvPath "Scripts" "python.exe"
$RequirementsFile = Join-Path $ScriptDir "requirements.txt"
$PythonScript = Join-Path $ScriptDir "sharepoint_backup.py"
$MonitorScript = Join-Path $ScriptDir "monitor.py"
$DatabasePath = Join-Path $ScriptDir "progress.db"
$LogPath = Join-Path $ScriptDir "sharepoint_backup.log"

# --- Helper Functions ---

# Write colored output to the console
function Write-ColorOutput($ForegroundColor, $Text) {
    Write-Host $Text -ForegroundColor $ForegroundColor
}

# Display the help message
function Show-Help {
    Write-ColorOutput Cyan "=== SharePoint Backup Tool - Help ==="
    Write-Host "This script helps you manage SharePoint backups on Windows."
    Write-Host ""
    Write-ColorOutput Yellow "Usage:"
    Write-Host "  .\sharepoint.ps1 [Action] [-Debug]"
    Write-Host ""
    Write-ColorOutput Yellow "Actions:"
    Write-Host "  setup      - Sets up the environment (virtualenv, dependencies)."
    Write-Host "  run        - Runs the main backup script."
    Write-Host "  monitor    - Monitors the backup progress."
    Write-Host "  stats      - Shows backup statistics from the local database."
    Write-Host "  clean      - Deletes the local database and log file."
    Write-Host "  help       - Shows this help message."
    Write-Host ""
    Write-ColorOutput Yellow "Options:"
    Write-Host "  -Debug     - Enables detailed debug output for the 'run' action."
    Write-Host ""
    Write-ColorOutput Yellow "Example:"
    Write-Host "  .\sharepoint.ps1 setup"
    Write-Host "  .\sharepoint.ps1 run"
}

# Check if Python 3.7+ is installed on the system
function Test-SystemPython {
    try {
        $pythonVersion = python --version 2>&1
        if ($pythonVersion -match "Python (\d+)\.(\d+)") {
            $major = [int]$matches[1]
            $minor = [int]$matches[2]
            if ($major -ge 3 -and $minor -ge 7) {
                Write-ColorOutput Green "✓ Python 3.7+ found on system."
                return $true
            } else {
                Write-ColorOutput Red "✗ Python 3.7+ is required, but found $($matches[0]). Please upgrade your Python installation."
                return $false
            }
        } else {
            Write-ColorOutput Red "✗ Could not determine Python version. Make sure Python 3.7+ is installed and in your PATH."
            return $false
        }
    } catch {
        Write-ColorOutput Red "✗ Python not found in PATH. Please install Python 3.7+ and ensure it's added to your PATH."
        return $false
    }
}

# Check if the Python virtual environment exists
function Test-VirtualEnv {
    if (Test-Path $PythonExecutable) {
        return $true
    }
    return $false
}

# Execute a command within the Python virtual environment
function Invoke-VenvCommand {
    param(
        [Parameter(Mandatory=$true)]
        [string[]]$Arguments
    )
    if (-not (Test-VirtualEnv)) {
        Write-ColorOutput Red "✗ Virtual environment not found. Please run 'setup' action first."
        exit 1
    }
    & $PythonExecutable @Arguments
    if ($LASTEXITCODE -ne 0) {
        Write-ColorOutput Red "✗ Command failed: $($Arguments -join ' ')"
        exit $LASTEXITCODE
    }
}

# --- Main Actions ---

function Setup-Environment {
    Write-ColorOutput Cyan "=== SharePoint Backup Tool Setup ==="

    if (-not (Test-SystemPython)) { exit 1 }

    if (Test-VirtualEnv) {
        Write-ColorOutput Green "✓ Virtual environment already exists."
    } else {
        Write-Host "`nCreating Python virtual environment..."
        python -m venv "$VenvPath"
        if ($LASTEXITCODE -eq 0) {
            Write-ColorOutput Green "✓ Virtual environment created successfully."
        } else {
            Write-ColorOutput Red "✗ Failed to create virtual environment."
            exit 1
        }
    }

    Write-Host "`nInstalling/Updating Python dependencies from $RequirementsFile..."
    Invoke-VenvCommand -Arguments "-m", "pip", "install", "--upgrade", "pip"
    Invoke-VenvCommand -Arguments "-m", "pip", "install", "-r", "$RequirementsFile"

    Write-Host "`nCreating sample configuration files (if they don't exist)..."
    Invoke-VenvCommand -Arguments "$PythonScript", "--create-config"

    Write-ColorOutput Green "`n✓ Setup complete!"
    Write-Host "`nNext steps:"
    Write-Host "1. Edit the configuration files:"
    Write-Host "   - credentials.txt (add your Azure AD app credentials)"
    Write-Host "   - sharepoint_sites.csv (add the SharePoint sites to back up)"
    Write-Host "   - config.ini (optional, review and adjust settings)"
    Write-Host "2. Run the backup: .\sharepoint.ps1 run"
}

function Run-Backup {
    Write-ColorOutput Cyan "=== Starting SharePoint Backup ==="
    if (-not (Test-Path "credentials.txt") -or -not (Test-Path "sharepoint_sites.csv")) {
        Write-ColorOutput Red "✗ Configuration files not found."
        Write-Host "Please ensure 'credentials.txt' and 'sharepoint_sites.csv' exist."
        Write-Host "Run 'setup' action to create sample files if needed."
        exit 1
    }

    $CommandArgs = @("$PythonScript")
    if ($DebugMode) {
        $CommandArgs += "--debug"
    }
    Invoke-VenvCommand -Arguments $CommandArgs
}

function Monitor-Backup {
    Write-ColorOutput Cyan "=== Monitoring Backup Progress ==="
    Invoke-VenvCommand -Arguments "$MonitorScript"
}

function Show-Stats {
    Write-ColorOutput Cyan "=== Backup Statistics ==="
    if (-not (Test-Path $DatabasePath)) {
        Write-ColorOutput Yellow "Database file not found. Run a backup first."
        exit 1
    }
    Invoke-VenvCommand -Arguments "$PythonScript", "--stats"
}

function Clean-Files {
    Write-ColorOutput Cyan "=== Cleaning Up Files ==="
    $filesToRemove = @($DatabasePath, $LogPath)
    foreach ($file in $filesToRemove) {
        if (Test-Path $file) {
            Write-Host "Removing $file..."
            Remove-Item $file -ErrorAction SilentlyContinue
            if ($?) {
                Write-ColorOutput Green "✓ Removed $file."
            } else {
                Write-ColorOutput Red "✗ Could not remove $file."
            }
        } else {
            Write-ColorOutput Yellow "✓ $file does not exist, skipping."
        }
    }
    Write-ColorOutput Green "`n✓ Cleanup complete."
}


# --- Main Script Logic ---
switch ($Action) {
    'setup'   { Setup-Environment }
    'run'     { Run-Backup }
    'monitor' { Monitor-Backup }
    'stats'   { Show-Stats }
    'clean'   { Clean-Files }
    'help'    { Show-Help }
    default   { Show-Help }
}