import logging
import threading
import time
from concurrent.futures import Future
from typing import Optional, Set


class EnqueueTracker:
    """
    Tracks pending enqueue operations for both async and sync dispatchers.
    
    For PerformanceDispatcher: tracks Future objects
    For SqlcDispatcher: tracks count of pending operations
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._lock = threading.Lock()
        self._futures: Set[Future] = set()
        self._pending_count = 0
        self._completion_event = threading.Event()
        self._completion_event.set()  # Start in completed state
        
    def track_future(self, future: Future) -> None:
        """Track a Future object (used by PerformanceDispatcher)."""
        with self._lock:
            self._futures.add(future)
            self._completion_event.clear()
            
        # Add callback to remove future when done
        def remove_future(f):
            with self._lock:
                self._futures.discard(f)
                self._check_completion()
                
        future.add_done_callback(remove_future)
        
    def increment_pending(self, count: int = 1) -> None:
        """Increment pending operation count (used by SqlcDispatcher)."""
        with self._lock:
            self._pending_count += count
            self._completion_event.clear()
            
    def decrement_pending(self, count: int = 1) -> None:
        """Decrement pending operation count (used by SqlcDispatcher)."""
        with self._lock:
            self._pending_count = max(0, self._pending_count - count)
            self._check_completion()
            
    def _check_completion(self) -> None:
        """Check if all operations are complete and set event if so."""
        # Must be called with lock held
        if not self._futures and self._pending_count == 0:
            self._completion_event.set()
            
    def wait_for_completion(self, timeout: Optional[float] = None) -> bool:
        """
        Wait for all tracked operations to complete.
        
        Args:
            timeout: Maximum time to wait in seconds. None means wait indefinitely.
            
        Returns:
            True if all operations completed, False if timeout occurred.
        """
        start_time = time.time()
        
        while True:
            with self._lock:
                # Check completion status
                done_futures = [f for f in self._futures if f.done()]
                pending_futures = len(self._futures) - len(done_futures)
                
                if pending_futures == 0 and self._pending_count == 0:
                    self.logger.debug("All enqueue operations completed")
                    return True
                    
                self.logger.debug(
                    f"Waiting for enqueue completion: {pending_futures} futures, "
                    f"{self._pending_count} pending operations"
                )
                
            # Check timeout
            if timeout is not None:
                elapsed = time.time() - start_time
                remaining = timeout - elapsed
                if remaining <= 0:
                    with self._lock:
                        failed_futures = [f for f in self._futures if not f.done()]
                        self.logger.warning(
                            f"Enqueue wait timeout after {timeout}s. "
                            f"{len(failed_futures)} futures pending, "
                            f"{self._pending_count} operations pending"
                        )
                    return False
                    
                # Wait with remaining timeout
                if self._completion_event.wait(min(remaining, 0.1)):
                    return True
            else:
                # Wait indefinitely in small increments
                if self._completion_event.wait(0.1):
                    return True
                    
    def get_failed_futures(self) -> list[Exception]:
        """Get exceptions from any failed futures."""
        exceptions = []
        with self._lock:
            for future in self._futures:
                if future.done():
                    try:
                        future.result()
                    except Exception as e:
                        exceptions.append(e)
        return exceptions