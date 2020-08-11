import pytest

from tests.helpers import set_env
from src.index_runner.main import _handle_msg
from src.utils.config import config


def test_handle_msg_skip_types():
    """
    Test that an event from a type from the blacklist results in a no-op in _handle_msg
    """
    with set_env(SKIP_TYPES='xyz'):
        config(force_reload=True)
        res = _handle_msg({'objtype': 'xyz', 'evtype': 'x'})
    assert res is None


def test_handle_msg_skip_types2():
    """
    Test that an event from a type NOT in the blacklist results in _handle_msg
    trying to handle the message
    """
    with set_env(SKIP_TYPES='xyz'):
        config(force_reload=True)
        with pytest.raises(RuntimeError) as ctx:
            _handle_msg({'objtype': 'abc', 'evtype': 'x'})
    assert str(ctx.value) == "Unrecognized event x."


def test_handle_msg_allow_types():
    """
    Test that an event from a type NOT IN the whitelist results in a no-op in _handle_msg
    """
    with set_env(ALLOW_TYPES='xyz'):
        config(force_reload=True)
        res = _handle_msg({'objtype': 'abc', 'evtype': 'x'})
    assert res is None


def test_handle_msg_allow_types2():
    """
    Test that an event from a type IN the whitelist results in _handle_msg
    trying to handle the message
    """
    with set_env(ALLOW_TYPES='xyz'):
        config(force_reload=True)
        with pytest.raises(RuntimeError) as ctx:
            _handle_msg({'objtype': 'xyz'})
    assert str(ctx.value) == "Missing 'evtype' in event: {'objtype': 'xyz'}"
