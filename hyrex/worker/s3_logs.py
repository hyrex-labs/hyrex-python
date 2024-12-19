import contextlib
import io
import sys
import threading

import boto3


def _upload_to_s3_sync(task_id: str, bucket_name: str, output: str):
    try:
        key = f"{task_id}.log"

        s3_client = boto3.client("s3")
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=output.encode("utf-8"))
    except Exception as e:
        print(f"Failed to upload task output to S3: {e}", file=sys.stderr)


@contextlib.contextmanager
def write_task_logs_to_s3(
    task_id: str, bucket_name: str, write_to_console: bool = False
):
    """
    Context manager to capture stdout/stderr and upload to S3.

    Args:
        task_id: Identifier for the task
        bucket_name: S3 bucket name for logs
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
            # Run in a separate thread to avoid blocking
            thread = threading.Thread(
                target=_upload_to_s3_sync, args=(task_id, bucket_name, output)
            )
            thread.daemon = True
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
