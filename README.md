# SharePoint Backup Tool

A robust Python tool for backing up / downloading SharePoint Online sites to local storage with SQLite progress tracking and automatic resume capability.

## Features

- 📁 **Bulk Download**: Download entire SharePoint sites and document libraries
- 🔄 **Resume Capability**: Automatically resume interrupted downloads using SQLite tracking
- ✅ **Site-level Tracking**: Marks sites as complete to avoid reprocessing
- 📊 **Progress Tracking**: Real-time progress monitoring with detailed statistics
- 🔐 **Secure Authentication**: Uses Azure AD app registration with client credentials
- 🧵 **Multi-threaded**: Concurrent downloads for improved performance
- 📝 **Detailed Logging**: Comprehensive logging with separate failed files log
- 💾 **Smart Storage**: Tracks file sizes in MB/GB for better readability
- 🔧 **Highly Configurable**: Environment variables, config file, or command-line options

## Prerequisites

- Python 3.7 or higher
- Azure AD application with SharePoint access
- Network access to SharePoint Online

## Installation

1. Clone the repository:
```bash
git clone https://github.com/Calvin-Zikakis/sharepoint-downloader
cd sharepoint-backup
```

2. Install required packages:
```bash
pip install O365
```

3. Create configuration files:
```bash
python sharepoint_backup.py --create-config
```

This creates three template files:
- `config.ini.sample` - Main configuration
- `sharepoint_sites.csv.sample` - List of SharePoint sites
- `credentials.txt.template` - Azure AD credentials

## Configuration

### 1. Azure AD Setup

Create an app registration in Azure Portal:
1. Go to Azure Active Directory → App registrations
2. New registration → Name your app
3. Under API permissions, add:
   - `Files.Read.All` (Application permission)
   - `Sites.Read.All` (Application permission)
4. Grant admin consent
5. Create a client secret under Certificates & secrets
6. Note your:
   - Application (client) ID
   - Directory (tenant) ID
   - Client secret value

### 2. Configure Credentials

Copy and edit the credentials file:
```bash
cp credentials.txt.template credentials.txt
```

Add your Azure AD details:
```
CLIENT_ID=your-client-id-here
CLIENT_SECRET=your-client-secret-here
TENANT_ID=your-tenant-id-here
```

### 3. Configure SharePoint Sites

Copy and edit the sites CSV:
```bash
cp sharepoint_sites.csv.sample sharepoint_sites.csv
```

Add your SharePoint sites:
```csv
SiteName,SiteURL
Marketing Team,https://yourcompany.sharepoint.com/sites/marketing
Sales Department,https://yourcompany.sharepoint.com/sites/sales
```

### 4. Optional: Configure Settings

Copy and edit the config file:
```bash
cp config.ini.sample config.ini
```

Or use environment variables:
```bash
export SHAREPOINT_OUTPUT_DIR=/path/to/backups
export SHAREPOINT_THREADS=10
export SHAREPOINT_LOG_LEVEL=DEBUG
```

## Usage

### Basic Usage

```bash
python sharepoint_backup.py
```

### With Custom Config

```bash
python sharepoint_backup.py --config /path/to/config.ini
```

### Enable Debug Mode

```bash
python sharepoint_backup.py --debug
```

### Site Management

```bash
# Show site completion status
python sharepoint_backup.py --show-site-status

# Reset a specific site to reprocess
python sharepoint_backup.py --reset-site "Marketing Team"
```

## Site-Level Tracking

The tool now tracks completion at the site level:

- **Completed Sites**: Automatically skipped on subsequent runs
- **Sites with Errors**: Only failed files are retried
- **Smart Resume**: After interruption, resumes exactly where it left off
- **Site Status**: View which sites are complete, partial, or pending

### Site Statuses

- ✅ **completed**: All files downloaded successfully
- ⚠️ **completed_with_errors**: Site processed but some files failed
- 🔄 **processing**: Currently being processed
- ⏳ **pending**: Queued for processing
- 📭 **empty**: No document libraries found

## Monitoring Progress

### Real-time Monitoring (PowerShell)

```powershell
# Monitor download progress
while($true) {
    Clear-Host
    sqlite3 progress.db "SELECT status, COUNT(*) FROM downloads GROUP BY status;"
    Start-Sleep -Seconds 5
}
```

### Database Queries

```sql
-- Check overall progress
SELECT status, COUNT(*) as count FROM downloads GROUP BY status;

-- View site completion status
SELECT site_name, status, total_files, completed_files, failed_files 
FROM sites ORDER BY site_name;

-- See failed files by site
SELECT site_name, COUNT(*) as failed_count 
FROM downloads 
WHERE status='failed' 
GROUP BY site_name;

-- Check size by site
SELECT site_name, 
       COUNT(*) as file_count,
       ROUND(SUM(file_size_mb), 2) as total_mb
FROM downloads 
GROUP BY site_name;

-- Find incomplete sites
SELECT site_name, status, total_files - completed_files as remaining 
FROM sites 
WHERE status != 'completed';

-- Current session statistics
SELECT * FROM statistics;
```

## Configuration Options

| Setting | Environment Variable | Description | Default |
|---------|---------------------|-------------|---------|
| OUTPUT_DIR | SHAREPOINT_OUTPUT_DIR | Download destination | ./sharepoint_backup |
| CSV_PATH | SHAREPOINT_CSV_PATH | Sites CSV file | ./sharepoint_sites.csv |
| NUM_THREADS | SHAREPOINT_THREADS | Concurrent downloads | 5 |
| MAX_RETRY | SHAREPOINT_MAX_RETRY | Retry attempts | 3 |
| LOG_LEVEL | SHAREPOINT_LOG_LEVEL | Logging verbosity | INFO |
| TOKEN_REFRESH_INTERVAL | SHAREPOINT_TOKEN_REFRESH | Token refresh (seconds) | 2700 |

## Troubleshooting

### Script Gets Stuck

If the script appears stuck:

1. Check for active downloads:
```bash
sqlite3 progress.db "SELECT COUNT(*) FROM downloads WHERE status='downloading';"
```

2. Kill and restart:
```powershell
Get-Process python* | Stop-Process -Force
python sharepoint_backup.py  # Will resume automatically
```

### Authentication Errors

1. Verify credentials in `credentials.txt`
2. Ensure app permissions are granted with admin consent
3. Check tenant ID is correct

### API Rate Limiting

The script handles rate limiting automatically, but you can adjust:
- Reduce `NUM_THREADS` in config
- Increase `API_RETRY_DELAY`

## Database Schema

### sites table
- `site_name`: SharePoint site name
- `site_url`: Original SharePoint URL
- `status`: pending/processing/completed/completed_with_errors
- `total_files`: Total files found in site
- `completed_files`: Successfully downloaded files
- `failed_files`: Failed download count
- `started_at`: When processing started
- `completed_at`: When site was completed

### downloads table
- `file_path`: Local destination path
- `site_name`: SharePoint site name
- `library_name`: Document library name
- `file_name`: Original filename
- `file_size_mb`: File size in megabytes
- `status`: discovered/downloading/completed/failed
- `error_message`: Error details if failed

### statistics table
- Tracks overall metrics like total downloads, failures, MB downloaded

### sessions table
- Tracks each run of the script with start/end times and statistics

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

MIT License - see LICENSE file for details

## Acknowledgments

- Uses the [O365](https://github.com/O365/python-o365) Python library
- SQLite for robust progress tracking

## Support

- Create an issue for bugs or feature requests
- Check existing issues before creating new ones
- Include logs and error messages when reporting issues