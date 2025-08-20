import contextlib
import logging
import re
import sys
from functools import lru_cache
from uuid import UUID

import boto3

from hyrex.dispatcher.performance_dispatcher import PerformanceDispatcher

# Compile regex once for performance
ANSI_ESCAPE_PATTERN = re.compile(r'\x1b\[[0-9;]*m')


def strip_ansi_codes(text: str) -> str:
    """Remove ANSI escape sequences from text."""
    return ANSI_ESCAPE_PATTERN.sub('', text)


@lru_cache(maxsize=1)
def get_s3_client():
    """Get or create cached S3 client"""
    return boto3.client("s3")


class LogCapture:
    """Simple log capture"""

    def __init__(self):
        self.logs = []

    def write(self, text):
        self.logs.append(text)
        return len(text)

    def flush(self):
        pass

    def getvalue(self):
        return "".join(self.logs)


class TeeIO:
    """Splits output between original stream and capture"""

    def __init__(self, original_stream, capture):
        self.original_stream = original_stream
        self.capture = capture

    def write(self, data):
        self.original_stream.write(data)
        self.capture.write(data)

    def flush(self):
        self.original_stream.flush()


def get_s3_key(task_id: str):
    return f"hyrex-logs/{task_id}.log"


async def _upload_to_s3_async(task_id: str, bucket_name: str, content: str):
    """
    Upload content to S3 asynchronously
    """
    try:
        key = get_s3_key(task_id=task_id)
        s3_client = get_s3_client()
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))
    except Exception as e:
        # TODO: Pass this up the chain
        print(f"Failed to upload logs to S3: {e}", file=sys.__stderr__)


@contextlib.asynccontextmanager
async def write_task_logs_to_s3(
    task_id: str,
    bucket_name: str,
    write_to_console: bool = True,
):
    """
    Async context manager for capturing and uploading task logs to S3.

    Args:
        task_id: Unique identifier for the task
        bucket_name: S3 bucket to store logs
        write_to_console: If True, also write output to console

    Yields:
        str: The S3 URL of the uploaded log file
    """
    log_capture = LogCapture()
    original_stdout = sys.stdout
    original_stderr = sys.stderr

    log_url = f"s3://{bucket_name}/{get_s3_key(task_id=task_id)}"

    # Only redirect logging if handlers exist (user is using logging module)
    root_logger = logging.getLogger()
    modified_handlers = []
    
    if root_logger.handlers:
        for handler in root_logger.handlers:
            if isinstance(handler, logging.StreamHandler):
                # Save original stream and redirect to our capture
                original_stream = handler.stream
                modified_handlers.append((handler, original_stream))
                if write_to_console:
                    handler.stream = TeeIO(original_stream, log_capture)
                else:
                    handler.stream = log_capture

    try:
        if write_to_console:
            sys.stdout = TeeIO(original_stdout, log_capture)
            sys.stderr = TeeIO(original_stderr, log_capture)
        else:
            sys.stdout = log_capture
            sys.stderr = log_capture
        yield log_url
    finally:
        # Restore original streams
        sys.stdout = original_stdout
        sys.stderr = original_stderr
        
        # Restore original handler streams
        for handler, original_stream in modified_handlers:
            handler.stream = original_stream

        # Upload logs if we captured anything
        content = log_capture.getvalue()
        # Strip ANSI color codes before uploading
        content = strip_ansi_codes(content)
        await _upload_to_s3_async(task_id, bucket_name, content)


@contextlib.asynccontextmanager
async def write_task_logs_with_dispatcher(
    task_id: UUID,
    dispatcher: PerformanceDispatcher,
    write_to_console: bool = True,
):
    """
    Async context manager for capturing and uploading task logs via dispatcher.

    Args:
        task_id: Unique identifier for the task
        dispatcher: PerformanceDispatcher instance with write_s3_logs method
        write_to_console: If True, also write output to console
    """
    log_capture = LogCapture()
    original_stdout = sys.stdout
    original_stderr = sys.stderr

    # Only redirect logging if handlers exist (user is using logging module)
    root_logger = logging.getLogger()
    modified_handlers = []
    
    if root_logger.handlers:
        for handler in root_logger.handlers:
            if isinstance(handler, logging.StreamHandler):
                # Save original stream and redirect to our capture
                original_stream = handler.stream
                modified_handlers.append((handler, original_stream))
                if write_to_console:
                    handler.stream = TeeIO(original_stream, log_capture)
                else:
                    handler.stream = log_capture

    try:
        if write_to_console:
            sys.stdout = TeeIO(original_stdout, log_capture)
            sys.stderr = TeeIO(original_stderr, log_capture)
        else:
            sys.stdout = log_capture
            sys.stderr = log_capture
        yield
    finally:
        # Restore original streams
        sys.stdout = original_stdout
        sys.stderr = original_stderr
        
        # Restore original handler streams
        for handler, original_stream in modified_handlers:
            handler.stream = original_stream

        # Upload logs via dispatcher
        content = log_capture.getvalue()
        # Strip ANSI color codes before uploading
        content = strip_ansi_codes(content)
        dispatcher.write_s3_logs(task_id, content)
