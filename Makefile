# SharePoint Backup Tool Makefile

.PHONY: help install setup run monitor clean test

# Default target
help:
	@echo "SharePoint Backup Tool - Available commands:"
	@echo "  make install    - Install required dependencies"
	@echo "  make setup      - Create sample configuration files"
	@echo "  make run        - Run the backup tool"
	@echo "  make monitor    - Run the monitoring dashboard"
	@echo "  make clean      - Clean up logs and temporary files"
	@echo "  make test       - Run tests"
	@echo "  make stats      - Show database statistics"

# Install dependencies
install:
	pip install -r requirements.txt

# Create sample configuration files
setup:
	python sharepoint_backup.py --create-config
	@echo ""
	@echo "Sample files created! Next steps:"
	@echo "1. Copy and edit the sample files:"
	@echo "   cp config.ini.sample config.ini"
	@echo "   cp sharepoint_sites.csv.sample sharepoint_sites.csv"
	@echo "   cp credentials.txt.template credentials.txt"
	@echo "2. Fill in your Azure AD credentials and SharePoint sites"
	@echo "3. Run 'make run' to start the backup"

# Run the backup tool
run:
	python sharepoint_backup.py

# Run with debug mode
debug:
	python sharepoint_backup.py --debug

# Run the monitor
monitor:
	python monitor.py

# Show statistics from the database
stats:
	@echo "=== Download Statistics ==="
	@sqlite3 progress.db "SELECT status, COUNT(*) as count FROM downloads GROUP BY status;" 2>/dev/null || echo "No database found"
	@echo ""
	@echo "=== Total by Site ==="
	@sqlite3 progress.db "SELECT site_name, COUNT(*) as files, ROUND(SUM(file_size_mb), 2) as mb FROM downloads GROUP BY site_name;" 2>/dev/null || echo "No database found"

# Clean up temporary files
clean:
	@echo "Cleaning up temporary files..."
	@rm -rf __pycache__ *.pyc
	@rm -rf logs/*.log
	@echo "Done! (Database and downloads are preserved)"

# Clean everything including database and downloads
clean-all: clean
	@echo "WARNING: This will delete all downloaded files and progress!"
	@echo "Press Ctrl+C to cancel, or wait 5 seconds to continue..."
	@sleep 5
	@rm -f progress.db progress.db-*
	@rm -rf sharepoint_backup/
	@rm -rf db_backups/
	@echo "All data cleaned!"

# Run tests (if you add them)
test:
	python -m pytest tests/

# Create a distribution package
dist:
	python setup.py sdist bdist_wheel

# For Windows users - PowerShell commands
ifeq ($(OS),Windows_NT)
monitor-ps:
	powershell -File monitor.ps1

stats-ps:
	@powershell -Command "sqlite3 progress.db 'SELECT status, COUNT(*) FROM downloads GROUP BY status;'"
endif