

def get_path(obj, path):
    """
    Fetch some deeply nested data from dicts and lists. Returns None if the
    path is not found in the data structure.
    Args:
        path - list of dict keys or list indices
    Example:
    get_path({'x': [1, 2, {'y': 3}]}, ['x', 2, 'y'])   returns 3
    get_path({'x': [1, 2, {'y': 3}]}, ['x', 'y'])      returns None
    """
    for part in path:
        if not isinstance(obj, list) and not isinstance(obj, dict):
            return obj
        try:
            obj[part]
        except Exception:
            return None
        obj = obj[part]
    return obj
