import os
import signal


def is_process_alive(pid):
    try:
        # Signal 0 is a special "null signal" - it tests existence of the process
        # without sending an actual signal. This is the standard way to check
        # process existence on Unix systems.
        os.kill(pid, 0)
        return True
    except ProcessLookupError:  # No process with this PID exists
        return False
    except PermissionError:  # Process exists but we don't have permission to signal it
        return True
