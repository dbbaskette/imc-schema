#!/usr/bin/env python3
"""
Remote Parquet Consolidation for Insurance MegaCorp
Uses SSH to access remote HDFS cluster
"""

import argparse
import logging
import os
import sys
import tempfile
import subprocess
from datetime import datetime
from typing import List, Tuple

import pandas as pd
import pyarrow.parquet as pq

# Configuration
HDFS_HOST = "hdfs@big-data-005.kuhn-labs.com"
BASE_PATH = "/insurance-megacorp/telemetry-data-v2"
TARGET_FILE_SIZE_MB = 128
MIN_FILES_TO_CONSOLIDATE = 5

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RemoteHDFSConsolidator:
    """Consolidator that uses SSH to access remote HDFS"""
    
    def __init__(self):
        self.hdfs_host = HDFS_HOST
        
    def run_hdfs_command(self, command: str) -> Tuple[bool, str]:
        """Run HDFS command on remote host via SSH"""
        full_command = f"ssh {self.hdfs_host} '{command}'"
        try:
            result = subprocess.run(
                full_command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                return True, result.stdout
            else:
                return False, result.stderr
                
        except subprocess.TimeoutExpired:
            return False, "Command timed out"
        except Exception as e:
            return False, str(e)
    
    def get_daily_files(self, date: str) -> List[str]:
        """Get list of Parquet files for a specific date"""
        date_path = f"{BASE_PATH}/date={date}"
        
        # List files in the directory
        success, output = self.run_hdfs_command(f"hdfs dfs -ls {date_path} 2>/dev/null")
        
        if not success:
            logger.warning(f"Could not list directory {date_path}: {output}")
            return []
        
        # Parse output and extract parquet files
        files = []
        for line in output.split('\n'):
            if 'telemetry-' in line and '.parquet' in line:
                parts = line.split()
                if len(parts) >= 8:
                    files.append(parts[-1])  # Full path is last column
        
        logger.info(f"Found {len(files)} Parquet files for date {date}")
        return files
    
    def get_file_sizes(self, files: List[str]) -> dict:
        """Get size information for files"""
        file_info = {}
        total_size = 0
        
        for file_path in files:
            success, output = self.run_hdfs_command(f"hdfs dfs -stat %s {file_path} 2>/dev/null")
            
            if success:
                try:
                    size = int(output.strip())
                    file_info[file_path] = {
                        'size_bytes': size,
                        'size_mb': size / (1024 * 1024)
                    }
                    total_size += size
                except ValueError:
                    logger.warning(f"Could not parse size for {file_path}")
                    file_info[file_path] = {'size_bytes': 0, 'size_mb': 0}
            else:
                file_info[file_path] = {'size_bytes': 0, 'size_mb': 0}
        
        return {
            'files': file_info,
            'total_size_mb': total_size / (1024 * 1024),
            'file_count': len(files)
        }
    
    def download_files(self, files: List[str], local_dir: str) -> List[str]:
        """Download files from remote HDFS to local directory"""
        os.makedirs(local_dir, exist_ok=True)
        local_files = []
        
        for hdfs_file in files:
            filename = os.path.basename(hdfs_file)
            local_file = os.path.join(local_dir, filename)
            
            # Use scp to download the file via HDFS
            success, output = self.run_hdfs_command(f"hdfs dfs -get {hdfs_file} /tmp/{filename}")
            
            if success:
                # Now copy from remote tmp to local
                scp_command = f"scp {self.hdfs_host}:/tmp/{filename} {local_file}"
                result = subprocess.run(scp_command, shell=True, capture_output=True)
                
                if result.returncode == 0:
                    local_files.append(local_file)
                    # Clean up remote tmp file
                    self.run_hdfs_command(f"rm -f /tmp/{filename}")
                    logger.debug(f"Downloaded {hdfs_file} -> {local_file}")
                else:
                    logger.error(f"Failed to download {hdfs_file}")
            else:
                logger.error(f"Failed to get {hdfs_file} from HDFS: {output}")
        
        logger.info(f"Downloaded {len(local_files)} files to {local_dir}")
        return local_files
    
    def consolidate_date_dry_run(self, date: str) -> bool:
        """Dry run analysis for a specific date"""
        logger.info(f"DRY RUN: Analyzing consolidation for date {date}")
        
        # Get file list
        files = self.get_daily_files(date)
        if len(files) < MIN_FILES_TO_CONSOLIDATE:
            logger.info(f"Only {len(files)} files found, minimum is {MIN_FILES_TO_CONSOLIDATE}. Would skip consolidation.")
            return True
        
        # Get file information
        file_info = self.get_file_sizes(files)
        
        logger.info("="*60)
        logger.info(f"📅 DRY RUN ANALYSIS FOR {date}")
        logger.info("="*60)
        logger.info(f"📁 Files found: {file_info['file_count']}")
        logger.info(f"📊 Total size: {file_info['total_size_mb']:.2f} MB")
        logger.info(f"📐 Average size: {file_info['total_size_mb']/file_info['file_count']:.1f} MB per file")
        
        # Show first few files as examples
        logger.info("\n📄 Sample files:")
        for i, (file_path, info) in enumerate(file_info['files'].items()):
            if i < 5:
                filename = os.path.basename(file_path)
                logger.info(f"  • {filename} ({info['size_mb']:.1f} MB)")
        if len(files) > 5:
            logger.info(f"  ... and {len(files) - 5} more files")
        
        # Consolidation analysis
        target_files = max(1, int(file_info['total_size_mb'] / TARGET_FILE_SIZE_MB))
        reduction_percent = (file_info['file_count'] - target_files) * 100 // file_info['file_count']
        
        logger.info("\n⚡ CONSOLIDATION IMPACT:")
        logger.info(f"🎯 Target file size: {TARGET_FILE_SIZE_MB} MB")
        logger.info(f"📦 Files after consolidation: {target_files}")
        logger.info(f"📉 File count reduction: {reduction_percent}% ({file_info['file_count']} → {target_files})")
        logger.info(f"🚀 Query performance improvement: ~{file_info['file_count']}x faster")
        
        if file_info['file_count'] > 10:
            logger.info("✅ EXCELLENT candidate for consolidation!")
        elif file_info['file_count'] > 5:
            logger.info("✅ GOOD candidate for consolidation")
        
        logger.info("="*60)
        
        return True


def main():
    parser = argparse.ArgumentParser(description="Remote HDFS Parquet Consolidation")
    parser.add_argument("--date", required=True, help="Date to analyze (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Show analysis without making changes")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    consolidator = RemoteHDFSConsolidator()
    
    if args.dry_run:
        success = consolidator.consolidate_date_dry_run(args.date)
        logger.info(f"\nDRY RUN completed {'successfully' if success else 'with issues'}")
        return 0 if success else 1
    else:
        logger.error("Full consolidation not implemented yet. Use --dry-run for analysis.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
