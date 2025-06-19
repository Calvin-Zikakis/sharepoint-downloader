#!/usr/bin/env python3
"""
SharePoint Backup Monitor
Real-time monitoring dashboard for SharePoint backup progress
"""

import sqlite3
import time
import os
from datetime import datetime
import argparse

# Default database path - can be overridden
DEFAULT_DB_PATH = "./progress.db"

class SharePointBackupMonitor:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = None
        
    def connect(self):
        """Connect to the database"""
        if not os.path.exists(self.db_path):
            print(f"Database not found: {self.db_path}")
            return False
        
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
            return True
        except Exception as e:
            print(f"Error connecting to database: {e}")
            return False
    
    def get_statistics(self):
        """Get current statistics from the database"""
        if not self.conn:
            return None
        
        stats = {}
        
        # Get site-level statistics
        cursor = self.conn.execute('''
            SELECT 
                COUNT(*) as total_sites,
                SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_sites,
                SUM(CASE WHEN status = 'completed_with_errors' THEN 1 ELSE 0 END) as sites_with_errors,
                SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) as processing_sites,
                SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending_sites
            FROM sites
        ''')
        site_summary = cursor.fetchone()
        stats['site_summary'] = site_summary if site_summary else {}
        
        # Get current processing site
        cursor = self.conn.execute('''
            SELECT site_name, total_files, completed_files, failed_files
            FROM sites 
            WHERE status = 'processing'
            ORDER BY last_updated DESC
            LIMIT 1
        ''')
        stats['current_site'] = cursor.fetchone()
        
        # Get file status counts
        cursor = self.conn.execute('''
            SELECT status, COUNT(*) as count 
            FROM downloads 
            GROUP BY status
            ORDER BY status
        ''')
        stats['status_counts'] = {row['status']: row['count'] for row in cursor}
        
        # Get overall statistics
        cursor = self.conn.execute('SELECT stat_name, stat_value FROM statistics')
        stats['metrics'] = {row['stat_name']: row['stat_value'] for row in cursor}
        
        # Get current session
        cursor = self.conn.execute('''
            SELECT * FROM sessions 
            WHERE end_time IS NULL 
            ORDER BY id DESC 
            LIMIT 1
        ''')
        stats['session'] = cursor.fetchone()
        
        # Get recent activity
        cursor = self.conn.execute('''
            SELECT COUNT(*) as count 
            FROM downloads 
            WHERE completed_at > datetime('now', '-5 minutes') 
                AND status = 'completed'
        ''')
        stats['recent_downloads'] = cursor.fetchone()['count']
        
        # Get currently downloading
        cursor = self.conn.execute('''
            SELECT COUNT(*) as count 
            FROM downloads 
            WHERE status = 'downloading'
        ''')
        stats['downloading_count'] = cursor.fetchone()['count']
        
        # Get recent failures
        cursor = self.conn.execute('''
            SELECT file_name, error_message, datetime(last_attempt, 'localtime') as last_attempt 
            FROM downloads 
            WHERE status = 'failed' 
                AND last_attempt > datetime('now', '-10 minutes')
            ORDER BY last_attempt DESC 
            LIMIT 5
        ''')
        stats['recent_failures'] = cursor.fetchall()
        
        return stats
    
    def format_size(self, size_mb):
        """Format size in MB to human readable format"""
        if size_mb < 1024:
            return f"{size_mb:.2f} MB"
        elif size_mb < 1024 * 1024:
            return f"{size_mb / 1024:.2f} GB"
        else:
            return f"{size_mb / (1024 * 1024):.2f} TB"
    
    def clear_screen(self):
        """Clear the console screen"""
        os.system('cls' if os.name == 'nt' else 'clear')
    
    def display_dashboard(self, stats):
        """Display the monitoring dashboard"""
        self.clear_screen()
        
        # Header
        print("=" * 80)
        print(f"SharePoint Backup Monitor - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        
        # Site Progress
        site_summary = stats.get('site_summary', {})
        if site_summary.get('total_sites', 0) > 0:
            print("\n🏢 SITE PROGRESS:")
            print("-" * 40)
            print(f"📊 Total sites      : {site_summary.get('total_sites', 0)}")
            print(f"✅ Completed       : {site_summary.get('completed_sites', 0)}")
            print(f"⚠️  With errors     : {site_summary.get('sites_with_errors', 0)}")
            print(f"🔄 Processing      : {site_summary.get('processing_sites', 0)}")
            print(f"⏳ Pending         : {site_summary.get('pending_sites', 0)}")
            
            # Current site being processed
            current_site = stats.get('current_site')
            if current_site:
                site_progress = 0
                if current_site['total_files'] > 0:
                    site_progress = (current_site['completed_files'] / current_site['total_files']) * 100
                
                print(f"\n📂 Current Site: {current_site['site_name']}")
                print(f"   Progress: {current_site['completed_files']}/{current_site['total_files']} files ({site_progress:.1f}%)")
                if current_site['failed_files'] > 0:
                    print(f"   ❌ Failed: {current_site['failed_files']} files")
        
        # File Status
        print("\n📊 FILE STATUS:")
        print("-" * 40)
        
        total_files = 0
        status_counts = stats.get('status_counts', {})
        
        # Define status display order and colors
        status_order = ['discovered', 'downloading', 'completed', 'failed']
        status_symbols = {
            'discovered': '🔍',
            'downloading': '⬇️ ',
            'completed': '✅',
            'failed': '❌'
        }
        
        for status in status_order:
            if status in status_counts:
                count = status_counts[status]
                total_files += count
                symbol = status_symbols.get(status, '  ')
                print(f"{symbol} {status.ljust(12)}: {count:,}")
        
        # Handle any other statuses
        for status, count in status_counts.items():
            if status not in status_order:
                total_files += count
                print(f"   {status.ljust(12)}: {count:,}")
        
        print(f"\n📁 TOTAL FILES   : {total_files:,}")
        
        # Download Statistics
        metrics = stats.get('metrics', {})
        print("\n📈 DOWNLOAD STATISTICS:")
        print("-" * 40)
        print(f"✅ Successful    : {int(metrics.get('successful_downloads', 0)):,}")
        print(f"❌ Failed        : {int(metrics.get('failed_downloads', 0)):,}")
        print(f"💾 Downloaded    : {self.format_size(metrics.get('mb_downloaded', 0))}")
        
        # Progress Bar
        if total_files > 0 and 'completed' in status_counts:
            completed = status_counts['completed']
            percent = (completed / total_files) * 100
            bar_width = 40
            filled = int((percent / 100) * bar_width)
            bar = '█' * filled + '░' * (bar_width - filled)
            print(f"\n📊 Overall Progress: [{bar}] {percent:.1f}%")
        
        # Current Activity
        print("\n⚡ CURRENT ACTIVITY:")
        print("-" * 40)
        print(f"📥 Files in last 5 min : {stats.get('recent_downloads', 0)}")
        print(f"⬇️  Currently downloading: {stats.get('downloading_count', 0)}")
        
        # Session Info
        session = stats.get('session')
        if session:
            start_time = datetime.strptime(session['start_time'], '%Y-%m-%d %H:%M:%S')
            runtime = datetime.now() - start_time
            hours, remainder = divmod(runtime.total_seconds(), 3600)
            minutes, seconds = divmod(remainder, 60)
            
            print(f"\n⏱️  SESSION INFO:")
            print("-" * 40)
            print(f"Runtime: {int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}")
            
            # Calculate rates
            if runtime.total_seconds() > 0 and metrics.get('successful_downloads', 0) > 0:
                files_per_hour = metrics['successful_downloads'] / (runtime.total_seconds() / 3600)
                mb_per_hour = metrics.get('mb_downloaded', 0) / (runtime.total_seconds() / 3600)
                print(f"Rate: {files_per_hour:.1f} files/hour ({self.format_size(mb_per_hour)}/hour)")
        
        # Recent Failures
        failures = stats.get('recent_failures', [])
        if failures:
            print(f"\n❌ RECENT FAILURES:")
            print("-" * 40)
            for failure in failures[:3]:
                print(f"• {failure['file_name']}")
                print(f"  Error: {failure['error_message'][:60]}...")
                print(f"  Time: {failure['last_attempt']}")
        
        print("\n" + "=" * 80)
        print("Refreshing every 5 seconds... Press Ctrl+C to exit")
    
    def run(self, refresh_interval=5):
        """Run the monitoring dashboard"""
        if not self.connect():
            return
        
        print("Starting SharePoint Backup Monitor...")
        print(f"Monitoring database: {self.db_path}")
        
        try:
            while True:
                stats = self.get_statistics()
                if stats:
                    self.display_dashboard(stats)
                else:
                    print("Error retrieving statistics")
                
                time.sleep(refresh_interval)
                
        except KeyboardInterrupt:
            print("\n\nMonitor stopped.")
        finally:
            if self.conn:
                self.conn.close()

def main():
    parser = argparse.ArgumentParser(description='Monitor SharePoint backup progress')
    parser.add_argument('--db', default=DEFAULT_DB_PATH, help='Path to progress.db')
    parser.add_argument('--interval', type=int, default=5, help='Refresh interval in seconds')
    parser.add_argument('--query', help='Run a single query and exit')
    
    args = parser.parse_args()
    
    if args.query:
        # Run a single query
        if not os.path.exists(args.db):
            print(f"Database not found: {args.db}")
            return
        
        conn = sqlite3.connect(args.db)
        try:
            cursor = conn.execute(args.query)
            # Print column headers
            if cursor.description:
                headers = [desc[0] for desc in cursor.description]
                print(" | ".join(headers))
                print("-" * (len(" | ".join(headers))))
            
            # Print rows
            for row in cursor:
                print(" | ".join(str(val) for val in row))
        except Exception as e:
            print(f"Query error: {e}")
        finally:
            conn.close()
    else:
        # Run the monitor
        monitor = SharePointBackupMonitor(args.db)
        monitor.run(args.interval)

if __name__ == "__main__":
    main()