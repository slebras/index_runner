from threading import Thread


def threadify(func, args):
    """
    Standardized way to start a thread from the event processor.
    """
    t = Thread(target=func, args=args)
    t.daemon = True
    t.start()
    return t
