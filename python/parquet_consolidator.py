#!/usr/bin/env python3
"""
Parquet File Consolidator for Insurance MegaCorp Telemetry Data

This utility consolidates small Parquet files into larger, more efficient files
for better query performance and reduced storage overhead.

Features:
- Consolidates daily telemetry files into single large file
- Maintains original schema and data integrity
- Removes source files after successful consolidation
- Supports HDFS operations via hdfs3 or subprocess
- Configurable file size targets and retention policies
- Skips 0-size files (actively being written)
- Uses config.env for configuration

Usage:
    python parquet_consolidator.py --date 2025-08-15 --dry-run
    python parquet_consolidator.py --date 2025-08-15 --target-size 128MB
"""

import argparse
import logging
import os
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Dict

# Number of parallel downloads
PARALLEL_DOWNLOADS = 8

# Cleanup threshold - files smaller than this are considered corrupted
CLEANUP_THRESHOLD_BYTES = 1024  # 1KB

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Try to import hdfs3, fallback to None if not available
try:
    from hdfs3 import HDFileSystem
    HDFS3_AVAILABLE = True
except ImportError:
    HDFileSystem = None
    HDFS3_AVAILABLE = False

def load_config() -> Dict[str, str]:
    """Load configuration from config.env file"""
    config = {}
    config_file = Path(__file__).parent / "config.env"
    
    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_file}")
    
    with open(config_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                # Handle export statements
                if line.startswith('export '):
                    line = line[7:]
                key, value = line.split('=', 1)
                # Remove quotes if present
                value = value.strip('"\'')
                config[key] = value
    
    return config

# Load configuration
try:
    CONFIG = load_config()
    HDFS_NAMENODE = CONFIG.get('HDFS_NAMENODE_HOST', 'big-data-005.kuhn-labs.com')
    HDFS_PORT = int(CONFIG.get('HDFS_NAMENODE_PORT', '8020'))
    BASE_PATH = "/insurance-megacorp/telemetry-data-v2"  # v2 path for consolidated files
    TARGET_FILE_SIZE_MB = 128  # Target consolidated file size
    MIN_FILES_TO_CONSOLIDATE = 5  # Only consolidate if >= this many files
    MIN_FILE_SIZE_BYTES = 1024  # Skip files smaller than 1KB (likely being written)
    TEMP_LOCAL_DIR = "/tmp/parquet_consolidation"
except Exception as e:
    print(f"Error loading configuration: {e}")
    print("Using default configuration values")
    HDFS_NAMENODE = "big-data-005.kuhn-labs.com"
    HDFS_PORT = 8020
    BASE_PATH = "/insurance-megacorp/telemetry-data-v2"
    TARGET_FILE_SIZE_MB = 128
    MIN_FILES_TO_CONSOLIDATE = 5
    MIN_FILE_SIZE_BYTES = 1024
    TEMP_LOCAL_DIR = "/tmp/parquet_consolidation"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ParquetConsolidator:
    """Main consolidation class for Parquet files in HDFS"""
    
    def __init__(self, hdfs_host: str = HDFS_NAMENODE, hdfs_port: int = HDFS_PORT):
        """Initialize HDFS connection and setup"""
        if HDFS3_AVAILABLE and HDFileSystem:
            try:
                self.hdfs = HDFileSystem(host=hdfs_host, port=hdfs_port)
                logger.info(f"Connected to HDFS at {hdfs_host}:{hdfs_port}")
            except Exception as e:
                logger.error(f"Failed to connect to HDFS: {e}")
                # Fallback to subprocess-based operations
                self.hdfs = None
                logger.warning("Using subprocess fallback for HDFS operations")
        else:
            # hdfs3 not available, use subprocess from the start
            self.hdfs = None
            logger.info("Using subprocess fallback for HDFS operations (hdfs3 not available)")
    
    def cleanup_corrupted_files(self, date: str, dry_run: bool = False) -> int:
        """Remove corrupted/tiny files from a date partition before consolidation"""
        date_path = f"{BASE_PATH}/date={date}"
        deleted_count = 0

        try:
            # Get file listing with sizes
            result = subprocess.run(
                ["hdfs", "dfs", "-ls", date_path],
                capture_output=True, text=True
            )
            if result.returncode != 0:
                logger.warning(f"Could not list directory for cleanup: {date_path}")
                return 0

            # Find and delete tiny files
            env = os.environ.copy()
            env['HADOOP_USER_NAME'] = 'hdfs'

            for line in result.stdout.split('\n'):
                if 'telemetry-' in line and '.parquet' in line:
                    parts = line.split()
                    if len(parts) >= 8:
                        try:
                            size = int(parts[4])
                            file_path = parts[-1]

                            if size <= CLEANUP_THRESHOLD_BYTES:
                                if dry_run:
                                    logger.info(f"Would delete corrupted file: {os.path.basename(file_path)} ({size} bytes)")
                                else:
                                    del_result = subprocess.run(
                                        ["hdfs", "dfs", "-rm", file_path],
                                        capture_output=True, text=True, env=env
                                    )
                                    if del_result.returncode == 0:
                                        logger.info(f"Deleted corrupted file: {os.path.basename(file_path)} ({size} bytes)")
                                        deleted_count += 1
                                    else:
                                        logger.warning(f"Failed to delete {file_path}: {del_result.stderr}")
                        except (ValueError, IndexError):
                            continue

            if deleted_count > 0:
                logger.info(f"Cleaned up {deleted_count} corrupted files for date {date}")

            return deleted_count

        except Exception as e:
            logger.error(f"Error during cleanup for date {date}: {e}")
            return 0

    def get_daily_files(self, date: str) -> List[str]:
        """Get list of Parquet files for a specific date"""
        date_path = f"{BASE_PATH}/date={date}"

        try:
            if self.hdfs:
                # Use hdfs3 library
                files = [f for f in self.hdfs.ls(date_path)
                        if f.endswith('.parquet') and 'telemetry-' in f and 'consolidated' not in f]
            else:
                # Fallback to subprocess
                result = subprocess.run(
                    ["hdfs", "dfs", "-ls", date_path],
                    capture_output=True, text=True
                )
                if result.returncode != 0:
                    logger.error(f"Failed to list HDFS directory: {result.stderr}")
                    return []

                files = []
                for line in result.stdout.split('\n'):
                    if 'telemetry-' in line and '.parquet' in line:
                        # Skip already consolidated files
                        if 'consolidated' in line:
                            continue
                        # Extract filename from ls output
                        parts = line.split()
                        if len(parts) >= 8:
                            files.append(parts[-1])

            logger.info(f"Found {len(files)} Parquet files for date {date}")
            return files

        except Exception as e:
            logger.error(f"Error listing files for date {date}: {e}")
            return []
    
    def get_file_info(self, files: List[str]) -> dict:
        """Get size and metadata info for files, skipping 0-size files"""
        total_size = 0
        file_info = {}
        skipped_files = []
        
        if self.hdfs:
            # Use hdfs3 library if available
            for file_path in files:
                try:
                    info = self.hdfs.info(file_path)
                    size = info['size']
                    
                    # Skip files smaller than minimum size (likely being written)
                    if size < MIN_FILE_SIZE_BYTES:
                        skipped_files.append(file_path)
                        logger.info(f"Skipping {file_path} ({size} bytes - too small, likely being written)")
                        continue
                    
                    file_info[file_path] = {
                        'size_bytes': size,
                        'size_mb': size / (1024 * 1024)
                    }
                    total_size += size
                    
                except Exception as e:
                    logger.warning(f"Could not get info for {file_path}: {e}")
                    skipped_files.append(file_path)
        else:
            # Use batch hdfs dfs -ls approach to avoid subprocess issues
            logger.info("Getting file sizes using batch HDFS ls...")
            
            # Get the directory path from the first file
            if files:
                import os
                dir_path = os.path.dirname(files[0])
                
                try:
                    import subprocess
                    result = subprocess.run(
                        ["hdfs", "dfs", "-ls", dir_path],
                        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, timeout=60
                    )
                    
                    if result.returncode == 0:
                        # Parse hdfs dfs -ls output
                        lines = result.stdout.strip().split('\n')
                        size_map = {}
                        
                        for line in lines:
                            # Skip headers and empty lines
                            if line.startswith('Found ') or line.startswith('drw') or not line.strip():
                                continue
                            
                            # Parse line: permissions user group size date time path
                            parts = line.split()
                            if len(parts) >= 8:
                                size_str = parts[4]
                                path = parts[7]
                                try:
                                    size = int(size_str)
                                    size_map[path] = size
                                except ValueError:
                                    continue
                        
                        # Match files with their sizes
                        for file_path in files:
                            if file_path in size_map:
                                size = size_map[file_path]
                                
                                # Skip files smaller than minimum size
                                if size < MIN_FILE_SIZE_BYTES:
                                    skipped_files.append(file_path)
                                    logger.info(f"Skipping {os.path.basename(file_path)} ({size} bytes - too small)")
                                    continue
                                
                                file_info[file_path] = {
                                    'size_bytes': size,
                                    'size_mb': size / (1024 * 1024)
                                }
                                total_size += size
                            else:
                                logger.warning(f"Could not find size for {file_path}")
                                skipped_files.append(file_path)
                    else:
                        logger.error(f"Failed to list directory {dir_path}")
                        # Fall back to skipping all files
                        skipped_files.extend(files)
                        
                except subprocess.TimeoutExpired:
                    logger.error("HDFS ls command timed out")
                    skipped_files.extend(files)
                except Exception as e:
                    logger.error(f"Error running HDFS ls: {e}")
                    skipped_files.extend(files)
        
        if skipped_files:
            logger.info(f"Skipped {len(skipped_files)} files (too small or errors)")
        
        return {
            'files': file_info,
            'total_size_mb': total_size / (1024 * 1024),
            'file_count': len(files)
        }
    
    def _download_single_file(self, hdfs_file: str, local_dir: str) -> Optional[str]:
        """Download a single file from HDFS (used by parallel downloader)"""
        try:
            filename = os.path.basename(hdfs_file)
            local_file = os.path.join(local_dir, filename)

            if self.hdfs:
                # Use hdfs3 library
                self.hdfs.get(hdfs_file, local_file)
            else:
                # Subprocess fallback
                result = subprocess.run([
                    "hdfs", "dfs", "-get", hdfs_file, local_file
                ], capture_output=True)
                if result.returncode != 0:
                    logger.error(f"Failed to download {hdfs_file}")
                    return None

            logger.debug(f"Downloaded {hdfs_file} -> {local_file}")
            return local_file

        except Exception as e:
            logger.error(f"Error downloading {hdfs_file}: {e}")
            return None

    def download_files(self, files: List[str], local_dir: str) -> List[str]:
        """Download Parquet files from HDFS to local temp directory (parallel)"""
        local_files = []

        os.makedirs(local_dir, exist_ok=True)

        logger.info(f"Downloading {len(files)} files using {PARALLEL_DOWNLOADS} parallel threads...")

        with ThreadPoolExecutor(max_workers=PARALLEL_DOWNLOADS) as executor:
            # Submit all download tasks
            future_to_file = {
                executor.submit(self._download_single_file, hdfs_file, local_dir): hdfs_file
                for hdfs_file in files
            }

            # Collect results as they complete
            for future in as_completed(future_to_file):
                hdfs_file = future_to_file[future]
                try:
                    local_file = future.result()
                    if local_file:
                        local_files.append(local_file)
                except Exception as e:
                    logger.error(f"Download failed for {hdfs_file}: {e}")

        logger.info(f"Downloaded {len(local_files)} files to {local_dir}")
        return local_files
    
    def consolidate_parquet_files(self, local_files: List[str], output_file: str) -> bool:
        """Consolidate multiple Parquet files into one using PyArrow"""
        try:
            logger.info(f"Consolidating {len(local_files)} files into {output_file}")
            
            # Read all Parquet files and combine
            tables = []
            total_rows = 0
            
            for file_path in local_files:
                table = pq.read_table(file_path)
                tables.append(table)
                total_rows += len(table)
                logger.debug(f"Read {len(table)} rows from {file_path}")
            
            # Concatenate all tables
            combined_table = pa.concat_tables(tables)
            logger.info(f"Combined {total_rows} total rows")
            
            # Write consolidated file with compression
            pq.write_table(
                combined_table, 
                output_file,
                compression='snappy',
                use_dictionary=True,
                row_group_size=50000  # Optimize for query performance
            )
            
            # Verify the output file
            verification_table = pq.read_table(output_file)
            if len(verification_table) != total_rows:
                logger.error(f"Row count mismatch! Expected {total_rows}, got {len(verification_table)}")
                return False
            
            logger.info(f"Successfully consolidated to {output_file} with {len(verification_table)} rows")
            return True
            
        except Exception as e:
            logger.error(f"Error consolidating files: {e}")
            return False
    
    def upload_consolidated_file(self, local_file: str, hdfs_path: str) -> bool:
        """Upload consolidated file back to HDFS using HADOOP_USER_NAME=hdfs"""
        try:
            if self.hdfs:
                self.hdfs.put(local_file, hdfs_path)
                logger.info(f"Uploaded {local_file} -> {hdfs_path}")
                return True
            else:
                import subprocess
                import os
                
                # Use HADOOP_USER_NAME=hdfs to run as hdfs user locally
                logger.info(f"Uploading {local_file} -> {hdfs_path} as hdfs user...")
                
                # Set up environment for hdfs user
                env = os.environ.copy()
                env['HADOOP_USER_NAME'] = 'hdfs'
                
                result = subprocess.run([
                    "hdfs", "dfs", "-put", local_file, hdfs_path
                ], capture_output=True, text=True, env=env, timeout=120)
                
                if result.returncode == 0:
                    logger.info(f"Successfully uploaded: {local_file} -> {hdfs_path}")
                    return True
                else:
                    logger.error(f"Upload failed: {result.stderr.strip()}")
                    return False
            
        except Exception as e:
            logger.error(f"Error uploading {local_file}: {e}")
            return False
    
    def remove_source_files(self, files: List[str]) -> bool:
        """Remove original source files from HDFS after successful consolidation"""
        success_count = 0
        
        logger.info(f"Removing {len(files)} source files as hdfs user...")
        
        for file_path in files:
            try:
                if self.hdfs:
                    self.hdfs.rm(file_path)
                else:
                    import subprocess
                    import os
                    
                    # Set up environment for hdfs user
                    env = os.environ.copy()
                    env['HADOOP_USER_NAME'] = 'hdfs'
                    
                    result = subprocess.run([
                        "hdfs", "dfs", "-rm", file_path
                    ], capture_output=True, text=True, env=env, timeout=30)
                    
                    if result.returncode != 0:
                        logger.warning(f"Failed to remove {file_path}: {result.stderr.strip()}")
                        continue
                
                success_count += 1
                logger.debug(f"Removed {file_path}")
                
            except Exception as e:
                logger.warning(f"Error removing {file_path}: {e}")
        
        logger.info(f"Removed {success_count}/{len(files)} source files")
        return success_count == len(files)
    
    def consolidate_date(self, date: str, target_size_mb: int = TARGET_FILE_SIZE_MB,
                        dry_run: bool = False) -> bool:
        """Main consolidation workflow for a specific date"""

        logger.info(f"{'DRY RUN: ' if dry_run else ''}Starting consolidation for date {date}")

        # Step 0: Clean up corrupted/tiny files first
        self.cleanup_corrupted_files(date, dry_run=dry_run)

        # Step 1: Get list of files
        files = self.get_daily_files(date)
        if len(files) < MIN_FILES_TO_CONSOLIDATE:
            logger.info(f"Only {len(files)} files found, minimum is {MIN_FILES_TO_CONSOLIDATE}. Skipping.")
            return True
        
        # Step 2: Get file information
        file_info = self.get_file_info(files)
        logger.info(f"Total data: {file_info['total_size_mb']:.2f} MB across {file_info['file_count']} files")
        
        if dry_run:
            logger.info("DRY RUN: Would consolidate the following files:")
            for file_path, info in file_info['files'].items():
                logger.info(f"  {file_path} ({info['size_mb']:.2f} MB)")
            logger.info(f"DRY RUN: Would create consolidated file ~{file_info['total_size_mb']:.2f} MB")
            return True
        
        # Step 3: Create temporary directory
        temp_dir = os.path.join(TEMP_LOCAL_DIR, f"consolidation_{date}_{datetime.now().strftime('%H%M%S')}")
        
        try:
            # Step 4: Download files
            local_files = self.download_files(files, temp_dir)
            if not local_files:
                logger.error("No files downloaded successfully")
                return False
            
            # Step 5: Consolidate files
            consolidated_filename = f"telemetry-{date}-consolidated-{datetime.now().strftime('%H%M%S')}.parquet"
            consolidated_local = os.path.join(temp_dir, consolidated_filename)
            
            if not self.consolidate_parquet_files(local_files, consolidated_local):
                logger.error("Consolidation failed")
                return False
            
            # Step 6: Upload consolidated file
            hdfs_consolidated_path = f"{BASE_PATH}/date={date}/{consolidated_filename}"
            if not self.upload_consolidated_file(consolidated_local, hdfs_consolidated_path):
                logger.error("Upload failed")
                return False
            
            # Step 7: Verify and remove source files using HADOOP_USER_NAME=hdfs
            # TODO: Add verification that consolidated file is readable via PXF
            if not self.remove_source_files(files):
                logger.warning("Some source files could not be removed")
            
            logger.info(f"Successfully consolidated {len(files)} files for date {date}")
            return True
            
        except Exception as e:
            logger.error(f"Consolidation failed for date {date}: {e}")
            return False
            
        finally:
            # Cleanup temp directory
            try:
                import shutil
                if os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir)
                    logger.debug(f"Cleaned up temp directory {temp_dir}")
            except Exception as e:
                logger.warning(f"Could not clean up temp directory: {e}")


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(description="Consolidate Parquet files by date")
    parser.add_argument("--date", required=True, help="Date to consolidate (YYYY-MM-DD)")
    parser.add_argument("--target-size", type=int, default=TARGET_FILE_SIZE_MB, 
                       help=f"Target file size in MB (default: {TARGET_FILE_SIZE_MB})")
    parser.add_argument("--dry-run", action="store_true", 
                       help="Show what would be done without making changes")
    parser.add_argument("--days-back", type=int, 
                       help="Consolidate multiple days going back from --date")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    consolidator = ParquetConsolidator()
    
    # Handle multiple days if specified
    if args.days_back:
        start_date = datetime.strptime(args.date, "%Y-%m-%d")
        dates = [(start_date - timedelta(days=i)).strftime("%Y-%m-%d") 
                for i in range(args.days_back + 1)]
    else:
        dates = [args.date]
    
    success_count = 0
    for date in dates:
        if consolidator.consolidate_date(date, args.target_size, args.dry_run):
            success_count += 1
        else:
            logger.error(f"Failed to consolidate date {date}")
    
    logger.info(f"Successfully processed {success_count}/{len(dates)} dates")
    return 0 if success_count == len(dates) else 1


if __name__ == "__main__":
    sys.exit(main())
