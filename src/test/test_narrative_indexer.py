"""
Tests specifically for the narrative indexer
"""
import os
import unittest

# from src.index_runner.es_indexers.narrative import index_narrative

_DIR = os.path.dirname(os.path.realpath(__file__))


class TestIndexers(unittest.TestCase):

    maxDiff = None

    def test_basic_valid(self):
        """Test the happy case."""
        # TODO Test basic valid narrative index

    def test_temporary_narr(self):
        """Test that temporary narratives get flagged."""
        # TODO test that temporary narratives get flagged

    def test_narratorial(self):
        """Test that a narratorial gets flagged as such."""
        # TODO test that a narrative gets flagged as such

    def test_fail_no_obj_metadata(self):
        """Test that a narrative index fails without obj metadata."""
        # TODO test that a narrative index fails without obj metadata

    def test_fail_no_ws_metadata(self):
        """Test that a narrative index fails without workspace metadata."""
        # TODO test that a narrative index fails without workspace metadata
