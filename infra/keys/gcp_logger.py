# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import io
import logging
from google.cloud import storage
from typing import List
from datetime import datetime

class GCSLogHandler(logging.Handler):
    """Custom logging handler that writes logs to Google Cloud Storage."""

    bucket_name: str
    log_file_prefix: str
    project_id: str
    storage_client: storage.Client
    bucket: storage.Bucket
    log_buffer: io.StringIO
    log_entries_count: int
    max_buffer_size: int
    session_logs: List[str]
    blob_name: str
    
    def __init__(self, bucket_name: str, log_file_prefix: str, project_id: str) -> None:
        """
        Initialize the GCS log handler.
        
        Args:
            bucket_name (str): Name of the GCS bucket
            log_file_prefix (str): Prefix for log file names
            project_id (str): Google Cloud project ID
        """
        super().__init__()
        self.bucket_name = bucket_name
        self.log_file_prefix = log_file_prefix
        self.project_id = project_id
        self.storage_client = storage.Client(project=project_id)
        self.bucket = self.storage_client.bucket(bucket_name)
        self.log_buffer = io.StringIO()
        self.log_entries_count = 0
        self.max_buffer_size = 100
        
        # Create a session-based filename that stays consistent
        session_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.blob_name = f"{self.log_file_prefix}/secret_service_{session_timestamp}.log"
        self.session_logs = []  # Store all logs for the session

    def emit(self, record: logging.LogRecord) -> None:
        """
        Emit a log record to the buffer and flush to GCS when buffer is full.
        
        Args:
            record: Log record to emit
        """
        try:
            log_entry = self.format(record)
            self.log_buffer.write(log_entry + '\n')
            self.session_logs.append(log_entry)
            self.log_entries_count += 1
            
            # Flush to GCS when buffer reaches max size
            if self.log_entries_count >= self.max_buffer_size:
                self.flush_to_gcs()
                
        except Exception:
            self.handleError(record)

    def flush_to_gcs(self) -> None:
        """Flush the log buffer to Google Cloud Storage."""
        if self.log_entries_count == 0:
            return
            
        try:
            # Upload all session logs to GCS
            blob = self.bucket.blob(self.blob_name)
            complete_log_content = '\n'.join(self.session_logs) + '\n'
            blob.upload_from_string(complete_log_content, content_type='text/plain')
            
            self.log_buffer = io.StringIO()
            self.log_entries_count = 0
            
        except Exception as e:
            # If GCS upload fails, print to stderr to avoid losing logs
            import sys
            print(f"Failed to upload logs to GCS: {e}", file=sys.stderr)
            print(f"Log file would be: {self.blob_name}", file=sys.stderr)
    
    def close(self) -> None:
        """Close the handler and flush remaining logs to GCS."""
        self.flush_to_gcs()
        super().close()

    def get_log_file_path(self) -> str:
        """Get the GCS path for the current log file."""
        return f"gs://{self.bucket_name}/{self.blob_name}"

class GCPLogger(logging.Logger):
    """Custom logger that uses GCSLogHandler to log messages to Google Cloud Storage."""

    def __init__(self, name: str, logging_level: str, bucket_name: str, log_file_prefix: str, project_id: str) -> None:
        """
        Initialize the GCP logger.
        
        Args:
            name (str): Name of the logger
            bucket_name (str): Name of the GCS bucket
            log_file_prefix (str): Prefix for log file names
            project_id (str): Google Cloud project ID
        """
        super().__init__(name)

        # Set the logging level, defaulting to INFO if not provided
        logging_level = logging_level.upper() if isinstance(logging_level, str) else logging_level
        if not logging_level or logging_level not in logging._nameToLevel:
            logging_level = 'INFO'
        self.setLevel(logging_level)
        
        # Clear any existing handlers to avoid duplicates
        self.handlers.clear()

        # Add console handler
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(console_formatter)
        self.addHandler(console_handler)

        # Add GCS handler for persistent logging
        try:
            gcs_handler = GCSLogHandler(bucket_name, log_file_prefix, project_id)
            gcs_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s')
            gcs_handler.setFormatter(gcs_formatter)
            self.addHandler(gcs_handler)
            self.info("GCS logging handler initialized successfully")
            self.info(f"Logs will be stored at: {gcs_handler.get_log_file_path()}")
        except Exception as e:
            self.warning(f"Failed to initialize GCS logging handler: {e}. Logs will only be written to console.")

