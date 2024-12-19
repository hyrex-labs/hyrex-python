import contextlib
import io
import sys
import threading
import atexit
import logging

import boto3

# Keep track of active upload threads
_upload_threads_lock = threading.Lock()
_upload_threads = []


def _upload_to_s3_sync(task_id: str, bucket_name: str, output: str):
    try:
        key = f"{task_id}.log"

        s3_client = boto3.client("s3")
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=output.encode("utf-8"))
    except Exception as e:
        print(f"Failed to upload task output to S3: {e}", file=sys.stderr)


def _upload_with_tracking(task_id: str, bucket_name: str, output: str):
    """Wrapper to remove thread from tracking list when done"""
    try:
        _upload_to_s3_sync(task_id, bucket_name, output)
    finally:
        current_thread = threading.current_thread()
        with _upload_threads_lock:
            if current_thread in _upload_threads:
                _upload_threads.remove(current_thread)


@atexit.register
def wait_for_uploads():
    """Wait for any pending upload threads to complete"""
    with _upload_threads_lock:
        threads_to_wait = _upload_threads[:]

    if threads_to_wait:
        print(f"Waiting for {len(_upload_threads)} log uploads to complete...")
        for thread in _upload_threads[:]:
            thread.join(timeout=3.0)  # Wait up to 3 seconds per thread


@contextlib.contextmanager
def write_task_logs_to_s3(
    task_id: str, bucket_name: str, write_to_console: bool = False
):
    """
    Context manager to capture stdout/stderr and upload to S3.

    Args:
        task_id: Identifier for the task
        bucket_name: S3 bucket name for logs
        write_to_console: Whether to also write output to console
    """
    buffer = io.StringIO()
    original_stdout = sys.stdout
    original_stderr = sys.stderr

    try:
        if write_to_console:
            sys.stdout = TeeIO(original_stdout, buffer)
            sys.stderr = TeeIO(original_stderr, buffer)
        else:
            sys.stdout = buffer
            sys.stderr = buffer
        yield
    finally:
        output = buffer.getvalue()
        sys.stdout = original_stdout
        sys.stderr = original_stderr

        if output:
            # Create and track non-daemon thread
            thread = threading.Thread(
                target=_upload_with_tracking, args=(task_id, bucket_name, output)
            )
            thread.daemon = False

            with _upload_threads_lock:
                _upload_threads.append(thread)
            thread.start()


class TeeIO:
    def __init__(self, original_stream, buffer):
        self.original_stream = original_stream
        self.buffer = buffer

    def write(self, data):
        self.original_stream.write(data)
        self.buffer.write(data)

    def flush(self):
        self.original_stream.flush()
        self.buffer.flush()
