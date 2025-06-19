# SharePoint Backup Tool - PowerShell Helper Script
# Run this script to manage your SharePoint backups on Windows

param(
    [Parameter(Position=0)]
    [ValidateSet('setup', 'run', 'monitor', 'stats', 'clean', 'help')]
    [string]$Action = 'help',
    
    [switch]$Debug
)

# Configuration
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PythonScript = Join-Path $ScriptDir "sharepoint_backup.py"
$MonitorScript = Join-Path $ScriptDir "monitor.py"
$DatabasePath = Join-Path $ScriptDir "progress.db"

# Colors for output
function Write-ColorOutput($ForegroundColor, $Text) {
    $fc = $host.UI.RawUI.ForegroundColor
    $host.UI.RawUI.ForegroundColor = $ForegroundColor
    Write-Host $Text
    $host.UI.RawUI.ForegroundColor = $fc
}

# Check if Python is installed
function Test-Python {
    try {
        $pythonVersion = python --version 2>&1
        if ($pythonVersion -match "Python (\d+)\.(\d+)") {
            $major = [int]$matches[1]
            $minor = [int]$matches[2]
            if ($major -ge 3 -and $minor -ge 7) {
                Write-ColorOutput Green "✓ Python $($matches[0]) found"
                return $true
            } else {
                Write-ColorOutput Red "✗ Python 3.7+ required (found $($matches[0]))"
                return $false
            }
        }
    } catch {
        Write-ColorOutput Red "✗ Python not found. Please install Python 3.7+"
        return $false
    }
}

# Check if SQLite is installed
function Test-SQLite {
    try {
        $null = sqlite3 --version 2>&1
        Write-ColorOutput Green "✓ SQLite found"
        return $true
    } catch {
        Write-ColorOutput Yellow "⚠ SQLite not found (optional, needed for direct DB queries)"
        return $false
    }
}

# Main actions
switch ($Action) {
    'setup' {
        Write-ColorOutput Cyan "=== SharePoint Backup Tool Setup ==="
        
        # Check prerequisites
        if (-not (Test-Python)) { exit 1 }
        
        # Install dependencies
        Write-Host "`nInstalling Python dependencies..."
        python -m pip install --upgrade pip
        python -m pip install O365
        
        # Create sample files
        Write-Host "`nCreating sample configuration files..."
        python $PythonScript --create-config
        
        Write-ColorOutput Green "`n✓ Setup complete!"
        Write-Host "`nNext steps:"
        Write-Host "1. Edit the configuration files:"
        Write-Host "   - credentials.txt (add your Azure AD credentials)"
        Write-Host "   - sharepoint_sites.csv (add your SharePoint sites)"
        Write-Host "   - config.ini (optional, adjust settings)"
        Write-Host "2. Run: .\sharepoint.ps1 run"
    }
    
    'run' {
        Write-ColorOutput Cyan "=== Starting SharePoint Backup ==="
        
        if (-not (Test-Python)) { exit 1 }
        
        # Check if credentials exist
        if (-not (Test-Path "credentials.txt")) {
            Write-ColorOutput Red "✗ credentials.txt not found. Run: .\sharepoint.ps1 setup"
            exit 1
        }
        
        # Run the backup
        if ($Debug) {
            python $PythonScript --debug
        } else {
            python $PythonScript
        }
    }
    
    'monitor' {
        Write-ColorOutput Cyan "=== SharePoint Backup Monitor ==="
        
        if (-not (Test-Python)) { exit 1 }
        
        if (-not (Test-Path $DatabasePath)) {
            Write-ColorOutput Red "✗ No database found. The backup needs to run first."
            exit 1
        }
        
        python $MonitorScript --db $DatabasePath
    }
    
    'stats' {
        Write-ColorOutput Cyan "=== SharePoint Backup Statistics ==="
        
        if (-not (Test-Path $DatabasePath)) {
            Write-ColorOutput Red "✗ No database found."
            exit 1
        }
        
        if (Test-SQLite) {
            # Use SQLite for detailed stats
            Write-Host "`nFile Status:"
            sqlite3 $DatabasePath "SELECT status, COUNT(*) as count FROM downloads GROUP BY status;"
            
            Write-Host "`nTop 5 Sites by File Count:"
            sqlite3 $DatabasePath "SELECT site_name, COUNT(*) as files, ROUND(SUM(file_size_mb), 2) as mb FROM downloads GROUP BY site_name ORDER BY files DESC LIMIT 5;"
            
            Write-Host "`nRecent Activity (last hour):"
            sqlite3 $DatabasePath "SELECT COUNT(*) as 'Files completed in last hour' FROM downloads WHERE completed_at > datetime('now', '-1 hour');"
        } else {
            # Use Python to show stats
            python -c @"
import sqlite3
conn = sqlite3.connect('$DatabasePath')
print('\nFile Status:')
for row in conn.execute('SELECT status, COUNT(*) FROM downloads GROUP BY status'):
    print(f'  {row[0]}: {row[1]}')
    
print('\nTotal Statistics:')
for row in conn.execute('SELECT stat_name, stat_value FROM statistics'):
    if row[0] == 'mb_downloaded':
        print(f'  {row[0]}: {row[1]:.2f} MB')
    else:
        print(f'  {row[0]}: {int(row[1])}')
conn.close()
"@
        }
    }
    
    'clean' {
        Write-ColorOutput Yellow "=== Cleaning Temporary Files ==="
        
        # Clean Python cache
        Remove-Item -Path "__pycache__" -Recurse -Force -ErrorAction SilentlyContinue
        Remove-Item -Path "*.pyc" -Force -ErrorAction SilentlyContinue
        
        # Clean old logs (keep last 5)
        if (Test-Path "logs") {
            $logs = Get-ChildItem "logs\*.log" | Sort-Object LastWriteTime -Descending | Select-Object -Skip 5
            $logs | Remove-Item -Force
            Write-Host "Cleaned $($logs.Count) old log files"
        }
        
        Write-ColorOutput Green "✓ Cleanup complete"
        Write-Host "Note: Database and downloaded files are preserved"
    }
    
    'help' {
        Write-ColorOutput Cyan @"
SharePoint Backup Tool - PowerShell Helper

Usage: .\sharepoint.ps1 [action] [-Debug]

Actions:
  setup    - Install dependencies and create configuration templates
  run      - Start the SharePoint backup process
  monitor  - Launch the real-time monitoring dashboard  
  stats    - Display backup statistics from the database
  clean    - Clean temporary files (preserves downloads and database)
  help     - Show this help message

Options:
  -Debug   - Enable debug logging (with 'run' action)

Examples:
  .\sharepoint.ps1 setup              # First-time setup
  .\sharepoint.ps1 run                # Start backup
  .\sharepoint.ps1 run -Debug         # Start with debug logging
  .\sharepoint.ps1 monitor            # Monitor progress
  .\sharepoint.ps1 stats              # View statistics

For more information, see README.md
"@
    }
}

# Add convenient aliases at the end
Write-Host "`nTip: You can also use these shortcuts:"
Write-Host "  Setup:   .\sharepoint.ps1 setup"
Write-Host "  Backup:  .\sharepoint.ps1 run"
Write-Host "  Monitor: .\sharepoint.ps1 monitor"