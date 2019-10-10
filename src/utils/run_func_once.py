"""
Utilities for running functions a limited amount of times.
"""


def run_once(fn):
    """
    Decorator that runs the function only once for a set of args. No-op on subsequent calls.
    Does not work with keyword args for now.
    """
    # Args we have already seen
    seen = set()  # type: set

    def _wrapper(*args):
        arg_str = str(args)
        if arg_str not in seen:
            seen.add(arg_str)
            fn(*args)
    return _wrapper
