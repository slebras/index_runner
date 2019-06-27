"""
Small manager of a group of processes.
"""
from dataclasses import dataclass, field
from typing import List, Tuple, Callable
from multiprocessing import Process


@dataclass
class WorkerGroup:
    fn: Callable  # function to run in each thread
    args: Tuple  # arguments for the above function
    count: int = 1  # how many threads to run
    workers: List[Process] = field(init=False)

    def __post_init__(self):
        """Start the threads."""
        self.workers = [_create_proc(self.fn, self.args) for _ in range(self.count)]

    def health_check(self):
        """Find any dead workers and restart them."""
        for (idx, proc) in enumerate(self.workers):
            if not proc.is_alive():
                print(f"Worker {proc} died, restarting..")
                self.workers[idx] = _create_proc(self.fn, self.args)

    def kill(self):
        """Kill all workers."""
        for proc in self.workers:
            proc.kill()


# -- Utilities

def _create_proc(func, args):
    """Create and start a new thread from a function and arguments."""
    proc = Process(target=func, args=args, daemon=True)
    proc.start()
    return proc
