"""
Small manager of a group of threads.
"""
from dataclasses import dataclass, field
from threading import Thread
from typing import List, Tuple, Callable


@dataclass
class ThreadGroup:
    fn: Callable  # function to run in each thread
    args: Tuple  # arguments for the above function
    count: int = 1  # how many threads to run
    threads: List[Thread] = field(init=False)

    def __post_init__(self):
        """Start the threads."""
        self.threads = [_threadify(self.fn, self.args) for _ in range(self.count)]

    def health_check(self):
        """Find any dead threads and restart them."""
        for (idx, thread) in enumerate(self.threads):
            if not thread.is_alive():
                print(f"Thread {self.fn} died, restarting..")
                self.threads[idx] = _threadify(self.fn, self.args)


# -- Utilities

def _threadify(func, args):
    """Create and start a new thread from a function and arguments."""
    t = Thread(target=func, args=args)
    t.daemon = True
    t.start()
    return t
