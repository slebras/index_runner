"""
Small manager of a group of processes with a work queue.

A worker group is a collection of 1 or more running processes.

The worker group receives messages from a queue. Messages are in the format
(method_name, method_data). The `method_name` gets dispatched to a method in
the provided class with the data as the arg (eg. `cls.method_name(method_data)`)

Some standard class attrs you can set:
    `init_children` - class method that is run before the process starts to
        initialize child WorkerGroups. This class method should return a
        dictionary of {name: worker_group_instance}. This will get set in the
        instance as `inst.children`, which is a dictionary of {name: worker_group_queue}.
        This way, your parent worker group can initialize child worker groups
        and send messages to them from within your methods.
    `on_queue_empty` - method that runs when the worker's queue goes empty.
"""
import traceback
from dataclasses import dataclass
from typing import Tuple, Callable
from multiprocessing import Process, Queue
import logging

logging.getLogger(__name__)


@dataclass
class WorkerGroup:
    cls: Callable   # class to run for each worker
    args: Tuple     # arguments for the above class
    count: int = 1  # how many workers to run

    def __post_init__(self):
        """Start the workers."""
        self.name = self.cls.__name__
        self.queue = Queue()  # type: Queue
        # Initialize child workers
        children = {}  # type: dict
        if hasattr(self.cls, 'init_children') and callable(self.cls.init_children):
            children = self.cls.init_children()
        # Fetch all the Queue objects for each initialized child
        child_queues = {key: worker.queue for (key, worker) in children.items()}
        # Start all of the worker processes
        self.workers = [_create_proc(self.cls, self.args, self.queue, child_queues) for _ in range(self.count)]

    def kill(self):
        """Kill all workers."""
        for proc in self.workers:
            proc.kill()


# -- Utilities

def _create_proc(cls, args, queue, child_queues):
    """Create and start a new process from a function and arguments."""
    proc = Process(target=_run_worker, args=(cls, args, queue, child_queues), daemon=True)
    proc.start()
    return proc


def _run_worker(cls, args, queue, child_queues):
    """
    Initialize and run an event loop within a child process for a worker class.
    This runs inside a child daemon process.
    It loops endlessly and dispatches messages to the worker group's class.
    It should (hopefully) catch all errors from any message handler, and it
    should not throw any errors itself, so that the process never crashes.
    """
    inst = cls(*args)
    # Link all child worker queues so inst can push work to them
    inst.children = {}
    for (worker_name, child_queue) in child_queues.items():
        inst.children[worker_name] = child_queue
    has_on_queue_empty = hasattr(inst, 'on_queue_empty') and callable(inst.on_queue_empty)
    while True:
        # Every item in the queue is a pair (method_name, method_data)
        (meth_name, msg_data) = queue.get()
        try:
            # Find the method and run it in the worker
            if hasattr(inst, meth_name) and callable(getattr(inst, meth_name)):
                meth = getattr(inst, meth_name)
                meth(msg_data)
            else:
                # Method not found
                raise RuntimeError(f"{cls.__name__} does not have a method '{meth_name}'")
        except Exception as err:
            _report_err(err)
        if queue.empty() and has_on_queue_empty:
            try:
                inst.on_queue_empty()
            except Exception as err:
                _report_err(err)


def _report_err(err):
    """Verbosely print an error caught in a worker's event loop."""
    logging.error('=' * 80 + '\n'
                  + str(err) + '\n'
                  + '-' * 80 + '\n'
                  + traceback.format_exc() + '\n'
                  + '=' * 80)
