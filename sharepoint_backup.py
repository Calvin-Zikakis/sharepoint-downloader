#!/usr/bin/env python3
"""
SharePoint Backup / Downlaoder Tool
A robust tool for backing up SharePoint sites to local storage with SQLite progress tracking.

Repository: https://github.com/Calvin-Zikakis/sharepoint-downloader
License: MIT
"""

import os
import sys
import csv
import logging
import time
import sqlite3
import json
import gc
import argparse
import configparser
from datetime import datetime, timedelta
from pathlib import Path
import threading
import queue
import shutil
from urllib.parse import urlparse
from collections import deque
from contextlib import contextmanager

# Try to load configuration from environment or config file
CONFIG_FILE = os.environ.get('SHAREPOINT_BACKUP_CONFIG', 'config.ini')

# Default Configuration - Override these with environment variables or config.ini
DEFAULT_CONFIG = {
    # Paths
    'OUTPUT_DIR': os.environ.get('SHAREPOINT_OUTPUT_DIR', './sharepoint_backup'),
    'CSV_PATH': os.environ.get('SHAREPOINT_CSV_PATH', './sharepoint_sites.csv'),
    'CREDENTIALS_PATH': os.environ.get('SHAREPOINT_CREDS_PATH', './credentials.txt'),
    'LOG_DIR': os.environ.get('SHAREPOINT_LOG_DIR', './logs'),
    'DB_PATH': os.environ.get('SHAREPOINT_DB_PATH', './progress.db'),
    
    # Performance Settings
    'NUM_THREADS': int(os.environ.get('SHAREPOINT_THREADS', '5')),
    'MAX_RETRY': int(os.environ.get('SHAREPOINT_MAX_RETRY', '3')),
    'BATCH_SIZE': int(os.environ.get('SHAREPOINT_BATCH_SIZE', '1000')),
    'MAX_FILES_PER_SESSION': int(os.environ.get('SHAREPOINT_MAX_FILES', '5000')),
    'API_RETRY_DELAY': int(os.environ.get('SHAREPOINT_API_DELAY', '30')),
    'TOKEN_REFRESH_INTERVAL': int(os.environ.get('SHAREPOINT_TOKEN_REFRESH', '2700')),  # 45 minutes
    'PROGRESS_SAVE_INTERVAL': int(os.environ.get('SHAREPOINT_PROGRESS_INTERVAL', '300')),  # 5 minutes
    'DB_BACKUP_INTERVAL': int(os.environ.get('SHAREPOINT_DB_BACKUP_INTERVAL', '600')),  # 10 minutes
    
    # Logging
    'LOG_LEVEL': os.environ.get('SHAREPOINT_LOG_LEVEL', 'INFO'),
    'DEBUG_MODE': os.environ.get('SHAREPOINT_DEBUG', 'False').lower() == 'true',
}

# Load configuration from file if it exists
config = DEFAULT_CONFIG.copy()
if os.path.exists(CONFIG_FILE):
    cfg_parser = configparser.ConfigParser()
    cfg_parser.read(CONFIG_FILE)
    if 'sharepoint_backup' in cfg_parser:
        config.update(dict(cfg_parser['sharepoint_backup']))
        # Convert string numbers to int
        for key in ['NUM_THREADS', 'MAX_RETRY', 'BATCH_SIZE', 'MAX_FILES_PER_SESSION', 
                    'API_RETRY_DELAY', 'TOKEN_REFRESH_INTERVAL', 'PROGRESS_SAVE_INTERVAL', 
                    'DB_BACKUP_INTERVAL']:
            if key in config and isinstance(config[key], str):
                config[key] = int(config[key])

# For handling O365 authentication and SharePoint operations
try:
    from O365 import Account
    from O365.sharepoint import Site
    import pkg_resources
    o365_version = pkg_resources.get_distribution("O365").version
    if o365_version < "2.0.0":
        print(f"Upgrading O365 library from version {o365_version} to the latest version...")
        import subprocess
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', '--upgrade', 'O365'])
        from O365 import Account
        from O365.sharepoint import Site
    print(f"Using O365 library version: {pkg_resources.get_distribution('O365').version}")
except ImportError:
    print("Required libraries not found. Installing...")
    import subprocess
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'O365'])
    from O365 import Account
    from O365.sharepoint import Site

# Create directories
for dir_path in [config['LOG_DIR'], os.path.dirname(config['DB_PATH']), config['OUTPUT_DIR']]:
    os.makedirs(dir_path, exist_ok=True)

# Setup logging
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = os.path.join(config['LOG_DIR'], f"sharepoint_backup_{timestamp}.log")

# Configure logging level
log_level = getattr(logging, config['LOG_LEVEL'].upper(), logging.INFO)
if config['DEBUG_MODE']:
    log_level = logging.DEBUG

logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Dedicated logger for failed file downloads
failed_files_log_name = f"sharepoint_backup_failed_files_{timestamp}.log"
failed_files_log_path = os.path.join(config['LOG_DIR'], failed_files_log_name)

failed_files_logger = logging.getLogger('FailedFilesLogger')
failed_files_logger.setLevel(logging.ERROR)

fh_failed = logging.FileHandler(failed_files_log_path)
fh_failed.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

failed_files_logger.addHandler(fh_failed)
failed_files_logger.propagate = False

# Progress tracking globals
file_counter = 0
file_counter_lock = threading.Lock()
last_token_refresh = datetime.now()
token_refresh_lock = threading.Lock()
last_db_backup = datetime.now()
db_backup_lock = threading.Lock()

# Global account object for token refresh
global_account = None
account_lock = threading.Lock()

# Database connection pool
db_local = threading.local()

def create_sample_config():
    """Create a sample configuration file"""
    sample_config = """[sharepoint_backup]
# Output directory for downloaded files
OUTPUT_DIR = ./sharepoint_backup

# Path to CSV file containing SharePoint sites
CSV_PATH = ./sharepoint_sites.csv

# Path to credentials file
CREDENTIALS_PATH = ./credentials.txt

# Directory for log files
LOG_DIR = ./logs

# SQLite database path
DB_PATH = ./progress.db

# Number of concurrent download threads
NUM_THREADS = 5

# Maximum retry attempts for failed downloads
MAX_RETRY = 3

# Batch size for processing files
BATCH_SIZE = 1000

# Maximum files to process before re-authenticating
MAX_FILES_PER_SESSION = 5000

# Delay in seconds when API errors occur
API_RETRY_DELAY = 30

# Token refresh interval in seconds (45 minutes)
TOKEN_REFRESH_INTERVAL = 2700

# Progress save interval in seconds
PROGRESS_SAVE_INTERVAL = 300

# Database backup interval in seconds
DB_BACKUP_INTERVAL = 600

# Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
LOG_LEVEL = INFO

# Enable debug mode
DEBUG_MODE = False
"""
    
    with open('config.ini.sample', 'w') as f:
        f.write(sample_config)
    logger.info("Created sample configuration file: config.ini.sample")

def create_sample_csv():
    """Create a sample SharePoint sites CSV file"""
    sample_csv = """SiteName,SiteURL
Marketing Team,https://yourcompany.sharepoint.com/sites/marketing
Sales Department,https://yourcompany.sharepoint.com/sites/sales
Engineering,https://yourcompany.sharepoint.com/sites/engineering
"""
    
    with open('sharepoint_sites.csv.sample', 'w') as f:
        f.write(sample_csv)
    logger.info("Created sample CSV file: sharepoint_sites.csv.sample")

def create_credentials_template():
    """Create a credentials template file"""
    template = """# SharePoint Backup Credentials
# Create an app registration in Azure AD with the following permissions:
# - Files.Read.All
# - Sites.Read.All

CLIENT_ID=your-client-id-here
CLIENT_SECRET=your-client-secret-here
TENANT_ID=your-tenant-id-here
"""
    
    template_path = config['CREDENTIALS_PATH'] + '.template'
    with open(template_path, 'w') as f:
        f.write(template)
    logger.info(f"Created credentials template: {template_path}")

@contextmanager
def get_db():
    """Get thread-local database connection"""
    if not hasattr(db_local, 'conn'):
        db_local.conn = sqlite3.connect(config['DB_PATH'], timeout=30.0)
        db_local.conn.row_factory = sqlite3.Row
        # Enable WAL mode for better concurrent access
        db_local.conn.execute("PRAGMA journal_mode=WAL")
        db_local.conn.execute("PRAGMA synchronous=NORMAL")
    
    try:
        yield db_local.conn
    except Exception as e:
        db_local.conn.rollback()
        raise
    else:
        db_local.conn.commit()

def init_database():
    """Initialize SQLite database with tables for progress tracking"""
    with get_db() as conn:
        # Main downloads table
        conn.execute('''
            CREATE TABLE IF NOT EXISTS downloads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_path TEXT UNIQUE NOT NULL,
                site_name TEXT,
                library_name TEXT,
                file_name TEXT,
                file_size_mb REAL,
                status TEXT NOT NULL,
                attempt_count INTEGER DEFAULT 0,
                last_attempt TIMESTAMP,
                completed_at TIMESTAMP,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create indexes for better query performance
        conn.execute('CREATE INDEX IF NOT EXISTS idx_status ON downloads(status)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_site ON downloads(site_name)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_library ON downloads(library_name)')
        
        # Statistics table
        conn.execute('''
            CREATE TABLE IF NOT EXISTS statistics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stat_name TEXT UNIQUE NOT NULL,
                stat_value REAL DEFAULT 0,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Session tracking table
        conn.execute('''
            CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                end_time TIMESTAMP,
                files_processed INTEGER DEFAULT 0,
                files_downloaded INTEGER DEFAULT 0,
                files_failed INTEGER DEFAULT 0,
                mb_downloaded REAL DEFAULT 0
            )
        ''')
        
        # Initialize statistics
        stats = [
            ('total_files', 0),
            ('successful_downloads', 0),
            ('failed_downloads', 0),
            ('skipped_existing', 0),
            ('api_errors', 0),
            ('mb_downloaded', 0)
        ]
        
        for stat_name, default_value in stats:
            conn.execute('''
                INSERT OR IGNORE INTO statistics (stat_name, stat_value) 
                VALUES (?, ?)
            ''', (stat_name, default_value))
        
        conn.commit()
        
    logger.info("Database initialized successfully")

def format_size(size_mb):
    """Format size in MB to human readable format"""
    if size_mb < 1024:
        return f"{size_mb:.2f} MB"
    elif size_mb < 1024 * 1024:
        return f"{size_mb / 1024:.2f} GB"
    else:
        return f"{size_mb / (1024 * 1024):.2f} TB"

def bytes_to_mb(bytes_size):
    """Convert bytes to megabytes"""
    return bytes_size / (1024 * 1024) if bytes_size else 0

def create_session():
    """Create a new session entry"""
    with get_db() as conn:
        cursor = conn.execute('INSERT INTO sessions DEFAULT VALUES')
        return cursor.lastrowid

def update_session(session_id, **kwargs):
    """Update session statistics"""
    with get_db() as conn:
        set_clauses = []
        values = []
        for key, value in kwargs.items():
            set_clauses.append(f"{key} = ?")
            values.append(value)
        
        values.append(session_id)
        query = f"UPDATE sessions SET {', '.join(set_clauses)} WHERE id = ?"
        conn.execute(query, values)

def get_file_status(file_path):
    """Check if a file has been downloaded or is in progress"""
    with get_db() as conn:
        result = conn.execute(
            'SELECT status, attempt_count FROM downloads WHERE file_path = ?',
            (file_path,)
        ).fetchone()
        
        if result:
            return result['status'], result['attempt_count']
        return None, 0

def update_statistics(stat_name, increment=1):
    """Update a statistic by incrementing its value"""
    with get_db() as conn:
        conn.execute('''
            UPDATE statistics 
            SET stat_value = stat_value + ?, last_updated = CURRENT_TIMESTAMP 
            WHERE stat_name = ?
        ''', (increment, stat_name))

def record_file_discovery(file_path, site_name, library_name, file_name, file_size_bytes=None):
    """Record when a file is discovered"""
    file_size_mb = bytes_to_mb(file_size_bytes) if file_size_bytes else None
    
    with get_db() as conn:
        # Check if this is a new file
        existing = conn.execute(
            'SELECT COUNT(*) FROM downloads WHERE file_path = ?',
            (file_path,)
        ).fetchone()[0]
        
        if existing == 0:
            conn.execute('''
                INSERT INTO downloads 
                (file_path, site_name, library_name, file_name, file_size_mb, status, attempt_count, created_at)
                VALUES (?, ?, ?, ?, ?, 'discovered', 0, CURRENT_TIMESTAMP)
            ''', (file_path, site_name, library_name, file_name, file_size_mb))
            
            # Update total_files count
            update_statistics('total_files')

def download_file(file_item, local_path, site_name, library_name):
    """Download a single file from SharePoint with improved error handling"""
    global file_counter
    
    # Increment file counter and check if we need to refresh token
    with file_counter_lock:
        file_counter += 1
        if file_counter % 100 == 0:
            refresh_token_if_needed()
            backup_database()
    
    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        # Check if file already exists and compare size
        if os.path.exists(local_path):
            local_size = os.path.getsize(local_path)
            try:
                remote_size = file_item.size if hasattr(file_item, 'size') else None
                if remote_size and local_size == remote_size:
                    update_download_status(local_path, 'completed', file_size_bytes=local_size)
                    # Update skipped_existing counter
                    update_statistics('skipped_existing')
                    logger.debug(f"File already exists with same size: {local_path}")
                    return True
            except Exception as e:
                logger.debug(f"Could not compare file sizes: {e}")
        
        # Update status from 'discovered' to 'downloading'
        with get_db() as conn:
            conn.execute('''
                UPDATE downloads 
                SET status = 'downloading', 
                    attempt_count = attempt_count + 1,
                    last_attempt = CURRENT_TIMESTAMP
                WHERE file_path = ?
            ''', (local_path,))
        
        # Download the file with retry logic
        for attempt in range(config['MAX_RETRY']):
            try:
                # Try to download
                response = file_item.download(to_path=os.path.dirname(local_path), name=os.path.basename(local_path))
                
                if response:
                    # Get actual file size after download
                    actual_size = os.path.getsize(local_path) if os.path.exists(local_path) else None
                    update_download_status(local_path, 'completed', file_size_bytes=actual_size)
                    
                    stats = get_statistics()
                    size_mb = bytes_to_mb(actual_size) if actual_size else 0
                    logger.info(f"Downloaded file ({stats['successful_downloads']}): {local_path} ({size_mb:.2f} MB)")
                    return True
                else:
                    logger.warning(f"Failed to download file (attempt {attempt+1}/{config['MAX_RETRY']}): {local_path}")
                    # Update api_errors counter
                    update_statistics('api_errors')
                    if attempt < config['MAX_RETRY'] - 1:
                        time.sleep(config['API_RETRY_DELAY'] * (attempt + 1))
                        
            except IndexError as e:
                error_msg = f"IndexError: {str(e)}"
                logger.error(f"IndexError downloading file (attempt {attempt+1}/{config['MAX_RETRY']}): {local_path}")
                # Update api_errors counter
                update_statistics('api_errors')
                
                if attempt < config['MAX_RETRY'] - 1:
                    logger.info(f"Waiting {config['API_RETRY_DELAY'] * (attempt + 1)} seconds before retry...")
                    time.sleep(config['API_RETRY_DELAY'] * (attempt + 1))
                    refresh_token_if_needed()
                else:
                    update_download_status(local_path, 'failed', error_msg)
                    
            except Exception as e:
                error_msg = str(e)
                logger.warning(f"Error downloading file (attempt {attempt+1}/{config['MAX_RETRY']}): {local_path}, Error: {error_msg}")
                # Update api_errors counter for API-related errors
                if any(keyword in error_msg.lower() for keyword in ['api', 'token', 'auth', 'timeout', 'connection']):
                    update_statistics('api_errors')
                    
                if attempt < config['MAX_RETRY'] - 1:
                    time.sleep(2 * (attempt + 1))
                else:
                    update_download_status(local_path, 'failed', error_msg)
        
        # All retries failed
        final_error = f"Failed after {config['MAX_RETRY']} attempts"
        update_download_status(local_path, 'failed', final_error)
        failed_files_logger.error(f"{final_error}: {local_path}")
        
        return False
        
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        update_download_status(local_path, 'failed', error_msg)
        logger.error(f"Unexpected error downloading file {getattr(file_item, 'name', 'unknown')}: {str(e)}")
        failed_files_logger.error(f"Unexpected error for {local_path}: {str(e)}")
        return False

def update_download_status(file_path, status, error_message=None, file_size_bytes=None):
    """Update the status of a download"""
    with get_db() as conn:
        if status == 'completed':
            file_size_mb = bytes_to_mb(file_size_bytes) if file_size_bytes else None
            
            conn.execute('''
                UPDATE downloads 
                SET status = ?, completed_at = CURRENT_TIMESTAMP, error_message = NULL,
                    file_size_mb = COALESCE(?, file_size_mb)
                WHERE file_path = ?
            ''', (status, file_size_mb, file_path))
            
            # Update statistics
            update_statistics('successful_downloads')
            
            if file_size_mb:
                update_statistics('mb_downloaded', file_size_mb)
                
        elif status == 'failed':
            conn.execute('''
                UPDATE downloads 
                SET status = ?, error_message = ?, last_attempt = CURRENT_TIMESTAMP 
                WHERE file_path = ?
            ''', (status, error_message, file_path))
            
            # Update statistics
            update_statistics('failed_downloads')

def get_statistics():
    """Get current statistics"""
    with get_db() as conn:
        stats = {}
        for row in conn.execute('SELECT stat_name, stat_value FROM statistics'):
            stats[row['stat_name']] = row['stat_value']
        
        # Get counts by status
        status_counts = conn.execute('''
            SELECT status, COUNT(*) as count 
            FROM downloads 
            GROUP BY status
        ''').fetchall()
        
        for row in status_counts:
            stats[f'status_{row["status"]}'] = row['count']
        
        return stats

def backup_database():
    """Backup the database to the output directory"""
    global last_db_backup
    
    with db_backup_lock:
        now = datetime.now()
        if (now - last_db_backup).total_seconds() > config['DB_BACKUP_INTERVAL']:
            try:
                backup_dir = os.path.join(config['OUTPUT_DIR'], 'db_backups')
                os.makedirs(backup_dir, exist_ok=True)
                
                backup_path = os.path.join(backup_dir, f'progress_backup_{now.strftime("%Y%m%d_%H%M%S")}.db')
                shutil.copy2(config['DB_PATH'], backup_path)
                logger.info(f"Database backed up to: {backup_path}")
                last_db_backup = now
                
                # Clean up old backups (keep last 5)
                backups = sorted([f for f in os.listdir(backup_dir) if f.startswith('progress_backup_')])
                for old_backup in backups[:-5]:
                    try:
                        os.remove(os.path.join(backup_dir, old_backup))
                    except:
                        pass
                        
            except Exception as e:
                logger.warning(f"Could not backup database: {e}")

def refresh_token_if_needed():
    """Refresh authentication token if needed"""
    global last_token_refresh, global_account
    
    with token_refresh_lock:
        now = datetime.now()
        if (now - last_token_refresh).total_seconds() > config['TOKEN_REFRESH_INTERVAL']:
            logger.info("Refreshing authentication token...")
            try:
                if global_account and hasattr(global_account.connection, 'refresh_token'):
                    global_account.connection.refresh_token()
                    last_token_refresh = now
                    logger.info("Token refreshed successfully")
                else:
                    logger.warning("Unable to refresh token - connection object not available")
            except Exception as e:
                logger.error(f"Error refreshing token: {e}")
                # Re-authenticate if refresh fails
                new_account = authenticate()
                if new_account:
                    with account_lock:
                        global_account = new_account
                    last_token_refresh = now

def authenticate():
    """Authenticate with Microsoft Graph API using client credentials flow"""
    # Check if credentials file exists
    if not os.path.exists(config['CREDENTIALS_PATH']):
        create_credentials_template()
        logger.error(f"Please edit the credentials file at {config['CREDENTIALS_PATH']}")
        logger.error("You can obtain these credentials from the Azure Portal by registering an application.")
        logger.error("Required permissions: Files.Read.All, Sites.Read.All")
        return None
    
    # Read credentials from file
    client_id = ""
    client_secret = ""
    tenant_id = ""
    
    with open(config['CREDENTIALS_PATH'], 'r') as f:
        for line in f:
            line = line.strip()
            if line.startswith('#') or not line:
                continue
            if line.startswith('CLIENT_ID='):
                client_id = line.split('=', 1)[1].strip()
            elif line.startswith('CLIENT_SECRET='):
                client_secret = line.split('=', 1)[1].strip()
            elif line.startswith('TENANT_ID='):
                tenant_id = line.split('=', 1)[1].strip()
    
    if not all([client_id, client_secret, tenant_id]):
        logger.error("Missing credentials in configuration file")
        return None
    
    try:
        # Set up auth scopes
        scopes = ['https://graph.microsoft.com/.default']
        
        # Connect to the Microsoft Graph API
        logger.info(f"Authenticating with tenant ID: {tenant_id}")
        credentials = (client_id, client_secret)
        
        # Create account
        account = Account(credentials, auth_flow_type='credentials', tenant_id=tenant_id, main_resource=tenant_id)
        
        # Authenticate
        if account.authenticate(scopes=scopes):
            logger.info("Authentication successful!")
            return account
        else:
            logger.error("Authentication failed!")
            return None
    except Exception as e:
        logger.error(f"Error during authentication: {str(e)}")
        return None

def read_sharepoint_sites():
    """Read SharePoint sites from CSV file"""
    if not os.path.exists(config['CSV_PATH']):
        create_sample_csv()
        logger.error(f"Please create a CSV file at {config['CSV_PATH']} with your SharePoint sites")
        logger.error("A sample file has been created: sharepoint_sites.csv.sample")
        return []
    
    sites = []
    try:
        with open(config['CSV_PATH'], 'r', encoding='utf-8-sig') as csvfile:  # Handle BOM
            reader = csv.DictReader(csvfile)
            for row in reader:
                sites.append({
                    'name': row.get('SiteName', '').strip(),
                    'url': row.get('SiteURL', '').strip()
                })
        logger.info(f"Successfully read {len(sites)} SharePoint sites from CSV")
        return sites
    except Exception as e:
        logger.error(f"Error reading SharePoint sites from CSV: {str(e)}")
        return []

def download_file(file_item, local_path, site_name, library_name):
    """Download a single file from SharePoint with improved error handling"""
    global file_counter
    
    # Increment file counter and check if we need to refresh token
    with file_counter_lock:
        file_counter += 1
        if file_counter % 100 == 0:
            refresh_token_if_needed()
            backup_database()
    
    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        # Check if file already exists and compare size
        if os.path.exists(local_path):
            local_size = os.path.getsize(local_path)
            try:
                remote_size = file_item.size if hasattr(file_item, 'size') else None
                if remote_size and local_size == remote_size:
                    update_download_status(local_path, 'completed', file_size_bytes=local_size)
                    # Update skipped_existing counter
                    update_statistics('skipped_existing')
                    logger.debug(f"File already exists with same size: {local_path}")
                    return True
            except Exception as e:
                logger.debug(f"Could not compare file sizes: {e}")
        
        # Update status from 'discovered' to 'downloading'
        with get_db() as conn:
            conn.execute('''
                UPDATE downloads 
                SET status = 'downloading', 
                    attempt_count = attempt_count + 1,
                    last_attempt = CURRENT_TIMESTAMP
                WHERE file_path = ?
            ''', (local_path,))
        
        # Download the file with retry logic
        for attempt in range(config['MAX_RETRY']):
            try:
                # Try to download
                response = file_item.download(to_path=os.path.dirname(local_path), name=os.path.basename(local_path))
                
                if response:
                    # Get actual file size after download
                    actual_size = os.path.getsize(local_path) if os.path.exists(local_path) else None
                    update_download_status(local_path, 'completed', file_size_bytes=actual_size)
                    
                    stats = get_statistics()
                    size_mb = bytes_to_mb(actual_size) if actual_size else 0
                    logger.info(f"Downloaded file ({stats['successful_downloads']}): {local_path} ({size_mb:.2f} MB)")
                    return True
                else:
                    logger.warning(f"Failed to download file (attempt {attempt+1}/{config['MAX_RETRY']}): {local_path}")
                    # Update api_errors counter
                    update_statistics('api_errors')
                    if attempt < config['MAX_RETRY'] - 1:
                        time.sleep(config['API_RETRY_DELAY'] * (attempt + 1))
                        
            except IndexError as e:
                error_msg = f"IndexError: {str(e)}"
                logger.error(f"IndexError downloading file (attempt {attempt+1}/{config['MAX_RETRY']}): {local_path}")
                # Update api_errors counter
                update_statistics('api_errors')
                
                if attempt < config['MAX_RETRY'] - 1:
                    logger.info(f"Waiting {config['API_RETRY_DELAY'] * (attempt + 1)} seconds before retry...")
                    time.sleep(config['API_RETRY_DELAY'] * (attempt + 1))
                    refresh_token_if_needed()
                else:
                    update_download_status(local_path, 'failed', error_msg)
                    
            except Exception as e:
                error_msg = str(e)
                logger.warning(f"Error downloading file (attempt {attempt+1}/{config['MAX_RETRY']}): {local_path}, Error: {error_msg}")
                # Update api_errors counter for API-related errors
                if any(keyword in error_msg.lower() for keyword in ['api', 'token', 'auth', 'timeout', 'connection']):
                    update_statistics('api_errors')
                    
                if attempt < config['MAX_RETRY'] - 1:
                    time.sleep(2 * (attempt + 1))
                else:
                    update_download_status(local_path, 'failed', error_msg)
        
        # All retries failed
        final_error = f"Failed after {config['MAX_RETRY']} attempts"
        update_download_status(local_path, 'failed', final_error)
        failed_files_logger.error(f"{final_error}: {local_path}")
        
        return False
        
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        update_download_status(local_path, 'failed', error_msg)
        logger.error(f"Unexpected error downloading file {getattr(file_item, 'name', 'unknown')}: {str(e)}")
        failed_files_logger.error(f"Unexpected error for {local_path}: {str(e)}")
        return False

def worker(q, session_id, site_name, library_name):
    """Worker thread to process download queue"""
    files_processed = 0
    files_downloaded = 0
    mb_downloaded = 0
    thread_name = threading.current_thread().name
    logger.debug(f"{thread_name} started for {site_name}/{library_name}")
    
    while True:
        job = None
        got_item = False
        
        try:
            job = q.get(timeout=30)  # 30 second timeout
            got_item = True  # Mark that we got an item
            
            if job is None:  # Poison pill to terminate thread
                logger.debug(f"{thread_name} received stop signal")
                break
                
            file_item, local_path = job
            logger.debug(f"{thread_name} processing: {local_path}")
            
            # Check if already downloaded
            status, attempt_count = get_file_status(local_path)
            if status == 'completed':
                logger.debug(f"Worker: Skipping (already completed): {local_path}")
            elif status == 'failed' and attempt_count >= config['MAX_RETRY']:
                logger.debug(f"Worker: Skipping (max retries reached): {local_path}")
            else:
                if download_file(file_item, local_path, site_name, library_name):
                    files_downloaded += 1
                    # Track MB downloaded
                    if os.path.exists(local_path):
                        mb_downloaded += bytes_to_mb(os.path.getsize(local_path))
                
            files_processed += 1
            
            # Update session stats periodically
            if files_processed % 10 == 0:
                update_session(session_id, 
                             files_processed=files_processed,
                             files_downloaded=files_downloaded,
                             mb_downloaded=mb_downloaded)
            
        except queue.Empty:
            logger.debug(f"{thread_name} timeout waiting for queue item")
            # Check if we should exit
            with get_db() as conn:
                active = conn.execute(
                    "SELECT COUNT(*) FROM downloads WHERE status='downloading'",
                ).fetchone()[0]
            
            if active == 0:
                logger.info(f"{thread_name} exiting - no active downloads")
                break
            continue
        except Exception as e:
            logger.error(f"Error in worker thread {thread_name}: {str(e)}", exc_info=True)
        finally:
            # CRITICAL: Always call task_done() if we got an item
            if got_item and job is not None:
                q.task_done()
    
    # Final update
    update_session(session_id, 
                   files_processed=files_processed,
                   files_downloaded=files_downloaded,
                   mb_downloaded=mb_downloaded)
    logger.debug(f"{thread_name} finished. Processed: {files_processed}, Downloaded: {files_downloaded}, MB: {mb_downloaded:.2f}")

def process_folder(folder, local_base_path, download_queue, site_name, library_name):
    """Process a folder and its contents recursively"""
    try:
        # Create the local folder
        local_folder_path = os.path.join(local_base_path, folder.name)
        os.makedirs(local_folder_path, exist_ok=True)
        
        files_discovered = 0
        
        # Process files in the folder with pagination
        for file_item in folder.get_items(limit=100):
            if file_item.is_folder:
                # Recursively process subfolders
                process_folder(file_item, local_folder_path, download_queue, site_name, library_name)
            else:
                # Add file to queue
                local_file_path = os.path.join(local_folder_path, file_item.name)
                
                # Check if already completed
                status, _ = get_file_status(local_file_path)
                if status != 'completed':
                    # Record file discovery immediately
                    try:
                        file_size = getattr(file_item, 'size', None)
                        record_file_discovery(local_file_path, site_name, library_name, file_item.name, file_size)
                        files_discovered += 1
                        
                        if files_discovered % 100 == 0:
                            logger.info(f"Discovered {files_discovered} files in {folder.name}")
                            
                    except Exception as e:
                        logger.debug(f"Could not record file discovery: {e}")
                    
                    download_queue.put((file_item, local_file_path))
        
        if files_discovered > 0:
            logger.info(f"Added {files_discovered} files from folder {folder.name} to download queue")
        
        return True
        
    except Exception as e:
        logger.error(f"Error processing folder {folder.name}: {str(e)}")
        return False

def backup_sharepoint_site(site_info, account, session_id):
    """Backup a SharePoint site with improved error handling"""
    site_name = site_info['name']
    original_site_url = site_info['url']
    
    try:
        # Parse the URL
        parsed_url = urlparse(original_site_url)
        hostname = parsed_url.hostname
        path = parsed_url.path
        
        # Ensure path starts with a slash
        if not path:
            path = '/'
        elif not path.startswith('/'):
            path = '/' + path
        
        if path.endswith('/') and len(path) > 1:
            path = path[:-1]
        
        # Construct the site_id
        if path and path != '/':
            site_id_for_o365 = f"{hostname}:{path}"
        else:
            site_id_for_o365 = hostname
        
        # Sanitize site name
        safe_site_name = "".join([c if (c.isalnum() or c in [' ', '-']) else "_" for c in site_name])
        local_site_path = os.path.join(config['OUTPUT_DIR'], safe_site_name)
        
        # Create site directory
        os.makedirs(local_site_path, exist_ok=True)
        
        # Get SharePoint site
        logger.info(f"Connecting to SharePoint site: {site_name} (URL: {original_site_url})")
        sharepoint = account.sharepoint()
        site = sharepoint.get_site(site_id_for_o365)
        
        if not site:
            logger.error(f"Failed to connect to site: {site_name}")
            return False
        
        # Get document libraries
        logger.info(f"Getting document libraries for site: {site_name}")
        document_libraries = site.list_document_libraries()
        
        if not document_libraries:
            logger.warning(f"No document libraries found for site: {site_name}")
            return False
        
        logger.info(f"Found {len(document_libraries)} document libraries in {site_name}")
        
        # Create download queue
        download_queue = queue.Queue(maxsize=config['BATCH_SIZE'] * 2)
        
        # Process each document library
        for lib_idx, doc_lib in enumerate(document_libraries):
            try:
                logger.info(f"\nProcessing document library {lib_idx+1}/{len(document_libraries)}: {doc_lib.name} in site: {site_name}")
                
                # Create local folder for this document library
                local_lib_path = os.path.join(local_site_path, doc_lib.name)
                os.makedirs(local_lib_path, exist_ok=True)
                
                # Create worker threads for this library
                threads = []
                for i in range(config['NUM_THREADS']):
                    t = threading.Thread(target=worker, 
                                       args=(download_queue, session_id, site_name, doc_lib.name))
                    t.daemon = True
                    t.start()
                    threads.append(t)
                
                # Get root folder of the document library
                logger.info(f"Scanning folders in {doc_lib.name}...")
                root_folder = doc_lib.get_root_folder()
                if root_folder:
                    # Process items in the root folder
                    items_found = 0
                    for item in root_folder.get_items(limit=100):
                        items_found += 1
                        if items_found % 50 == 0:
                            logger.info(f"Scanning... found {items_found} items so far in {doc_lib.name}")
                            
                        if item.is_folder:
                            process_folder(item, local_lib_path, download_queue, site_name, doc_lib.name)
                        else:
                            local_file_path = os.path.join(local_lib_path, item.name)
                            
                            # Check if already completed
                            status, _ = get_file_status(local_file_path)
                            if status != 'completed':
                                # Record file discovery
                                try:
                                    file_size = getattr(item, 'size', None)
                                    record_file_discovery(local_file_path, site_name, doc_lib.name, item.name, file_size)
                                except Exception as e:
                                    logger.debug(f"Could not record file discovery: {e}")
                                
                                download_queue.put((item, local_file_path))
                
                logger.info(f"Completed scanning {doc_lib.name}. Starting downloads...")
                
                # Print current statistics
                print_statistics()
                
                # Wait for downloads to complete without using queue.join()
                logger.info(f"Waiting for downloads to complete in {doc_lib.name}...")
                
                # First, wait for queue to be empty
                while not download_queue.empty():
                    time.sleep(1)
                
                # Then check if any downloads are still active
                active_downloads = True
                timeout_counter = 0
                max_timeout = 60  # 60 seconds max wait after queue is empty
                
                while active_downloads and timeout_counter < max_timeout:
                    with get_db() as conn:
                        downloading_count = conn.execute(
                            "SELECT COUNT(*) FROM downloads WHERE status='downloading' AND library_name=?",
                            (doc_lib.name,)
                        ).fetchone()[0]
                    
                    if downloading_count == 0:
                        active_downloads = False
                        logger.info(f"All downloads completed for {doc_lib.name}")
                    else:
                        logger.info(f"Still downloading {downloading_count} files in {doc_lib.name}...")
                        time.sleep(5)
                        timeout_counter += 5
                
                # Stop workers
                logger.info("Stopping worker threads...")
                for i in range(config['NUM_THREADS']):
                    download_queue.put(None)
                
                # Wait for threads with timeout
                for idx, t in enumerate(threads):
                    t.join(timeout=10)
                    if t.is_alive():
                        logger.warning(f"Thread {idx} did not stop cleanly")
                
                logger.info(f"Completed processing library {doc_lib.name}")
                    
            except Exception as e:
                logger.error(f"Error processing document library {doc_lib.name}: {str(e)}")
                continue
        
        logger.info(f"Completed backup of site: {site_name}")
        return True
        
    except Exception as e:
        logger.error(f"Error backing up SharePoint site {site_name}: {str(e)}")
        return False

def verify_output_path():
    """Verify that the output path is accessible"""
    try:
        # Try to create the output directory
        os.makedirs(config['OUTPUT_DIR'], exist_ok=True)
        
        # Test write permissions
        test_file = os.path.join(config['OUTPUT_DIR'], ".test_write_access")
        try:
            with open(test_file, 'w') as f:
                f.write("test")
            os.remove(test_file)
            return True
        except (IOError, PermissionError) as e:
            logger.error(f"Cannot write to {config['OUTPUT_DIR']}. Error: {str(e)}")
            return False
    except Exception as e:
        logger.error(f"Error verifying output path: {str(e)}")
        return False

def print_statistics():
    """Print download statistics"""
    stats = get_statistics()
    
    logger.info("\n" + "="*50)
    logger.info("DOWNLOAD STATISTICS")
    logger.info("="*50)
    
    # Get detailed status counts
    with get_db() as conn:
        status_counts = conn.execute('''
            SELECT status, COUNT(*) as count 
            FROM downloads 
            GROUP BY status
            ORDER BY status
        ''').fetchall()
        
        total_files = sum(row['count'] for row in status_counts)
        logger.info(f"Total files tracked: {total_files}")
        
        for row in status_counts:
            logger.info(f"  {row['status']}: {row['count']}")
    
    logger.info(f"\nSuccessful downloads: {stats.get('successful_downloads', 0)}")
    logger.info(f"Failed downloads: {stats.get('failed_downloads', 0)}")
    logger.info(f"Data downloaded: {format_size(stats.get('mb_downloaded', 0))}")
    
    # Show current activity
    with get_db() as conn:
        downloading = conn.execute('''
            SELECT COUNT(*) as count 
            FROM downloads 
            WHERE status = 'downloading'
        ''').fetchone()
        
        if downloading and downloading['count'] > 0:
            logger.info(f"\nCurrently downloading: {downloading['count']} files")
    
    # Get failed files for summary
    with get_db() as conn:
        failed_files = conn.execute('''
            SELECT file_path, attempt_count, error_message 
            FROM downloads 
            WHERE status = 'failed' AND attempt_count >= ?
            LIMIT 10
        ''', (config['MAX_RETRY'],)).fetchall()
        
        if failed_files:
            logger.info(f"\nFailed files (showing first 10):")
            for file in failed_files:
                logger.info(f"  - {file['file_path']}")
                logger.info(f"    Attempts: {file['attempt_count']}, Error: {file['error_message']}")
    
    logger.info("="*50 + "\n")

def fix_statistics():
    """Fix/recalculate statistics from existing data"""
    with get_db() as conn:
        # Recalculate total_files
        total = conn.execute('SELECT COUNT(*) FROM downloads').fetchone()[0]
        conn.execute('UPDATE statistics SET stat_value = ? WHERE stat_name = "total_files"', (total,))
        
        # Recalculate successful_downloads
        successful = conn.execute('SELECT COUNT(*) FROM downloads WHERE status = "completed"').fetchone()[0]
        conn.execute('UPDATE statistics SET stat_value = ? WHERE stat_name = "successful_downloads"', (successful,))
        
        # Recalculate failed_downloads
        failed = conn.execute('SELECT COUNT(*) FROM downloads WHERE status = "failed"').fetchone()[0]
        conn.execute('UPDATE statistics SET stat_value = ? WHERE stat_name = "failed_downloads"', (failed,))
        
        # Recalculate mb_downloaded
        mb_total = conn.execute('SELECT COALESCE(SUM(file_size_mb), 0) FROM downloads WHERE status = "completed"').fetchone()[0]
        conn.execute('UPDATE statistics SET stat_value = ? WHERE stat_name = "mb_downloaded"', (mb_total,))
        
        logger.info("Statistics recalculated from database")

def main():
    parser = argparse.ArgumentParser(description='SharePoint Backup Tool')
    parser.add_argument('--config', help='Path to config file', default='config.ini')
    parser.add_argument('--create-config', action='store_true', help='Create sample configuration files')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--fix-stats', action='store_true', help='Recalculate statistics from database')
    args = parser.parse_args()
    
    if args.fix_stats:
        init_database()
        fix_statistics()
        print_statistics()
        return
    
    if args.create_config:
        create_sample_config()
        create_sample_csv()
        create_credentials_template()
        print("\nSample files created:")
        print("  - config.ini.sample: Configuration file template")
        print("  - sharepoint_sites.csv.sample: SharePoint sites CSV template")
        print("  - credentials.txt.template: Credentials file template")
        print("\nNext steps:")
        print("1. Copy and edit the sample files (remove .sample/.template extension)")
        print("2. Fill in your SharePoint sites and Azure AD credentials")
        print("3. Run the script again without --create-config")
        return
    
    if args.debug:
        config['DEBUG_MODE'] = True
        logger.setLevel(logging.DEBUG)
    
    logger.info("="*70)
    logger.info("SharePoint Backup Tool - Starting")
    logger.info("="*70)
    logger.info(f"Configuration:")
    logger.info(f"  Output Directory: {config['OUTPUT_DIR']}")
    logger.info(f"  Database Path: {config['DB_PATH']}")
    logger.info(f"  Thread Count: {config['NUM_THREADS']}")
    logger.info(f"  Log Level: {config['LOG_LEVEL']}")
    logger.info("="*70)
    
    # Initialize database
    init_database()
    
    # Create new session
    session_id = create_session()
    logger.info(f"Created session ID: {session_id}")
    
    # Verify output path is accessible
    if not verify_output_path():
        logger.error("Output directory validation failed. Cannot proceed.")
        return
    
    # Authenticate
    account = authenticate()
    if not account:
        logger.error("Authentication failed. Cannot proceed.")
        logger.error("Please check your credentials and try again.")
        return
    
    # Set global account for token refresh
    with account_lock:
        global_account = account
    
    # Read SharePoint sites from CSV
    sites = read_sharepoint_sites()
    if not sites:
        logger.error("No SharePoint sites found in CSV file. Cannot proceed.")
        return
    
    # Process each SharePoint site
    success_count = 0
    start_time = datetime.now()
    
    for idx, site_info in enumerate(sites):
        logger.info(f"\nProcessing site {idx+1}/{len(sites)}: {site_info['name']}")
        logger.info("="*70)
        
        try:
            if backup_sharepoint_site(site_info, account, session_id):
                success_count += 1
            
            # Print progress stats
            print_statistics()
            
            # Re-authenticate periodically
            if (idx + 1) % 5 == 0:
                logger.info("Re-authenticating to refresh token...")
                account = authenticate()
                if account:
                    with account_lock:
                        global_account = account
                else:
                    logger.error("Re-authentication failed. Stopping.")
                    break
                    
        except Exception as e:
            logger.error(f"Unexpected error processing site {site_info['name']}: {e}")
            continue
        
        # Free memory between sites
        gc.collect()
    
    # Update session end time
    update_session(session_id, end_time=datetime.now().isoformat())
    
    # Final statistics
    elapsed = datetime.now() - start_time
    logger.info("\n" + "="*70)
    logger.info(f"FINAL SUMMARY: Backup process completed in {elapsed}")
    logger.info(f"Successfully backed up {success_count}/{len(sites)} sites")
    logger.info(f"Database location: {config['DB_PATH']}")
    logger.info(f"Backup location: {config['OUTPUT_DIR']}")
    logger.info(f"Log file: {log_file}")
    print_statistics()
    
    # Backup database one final time
    backup_database()
    
    # Show how to query the database
    logger.info("\nTo query the database for specific information:")
    logger.info(f"  sqlite3 \"{config['DB_PATH']}\"")
    logger.info("  Example queries:")
    logger.info("    SELECT * FROM downloads WHERE status = 'failed';")
    logger.info("    SELECT site_name, COUNT(*), SUM(file_size_mb) FROM downloads GROUP BY site_name;")
    logger.info("    SELECT * FROM statistics;")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\nScript interrupted by user. Progress is saved in database.")
        print_statistics()
    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)