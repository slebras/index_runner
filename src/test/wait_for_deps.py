"""
Used in run_tests.sh to wait for various service dependencies (arango, elastic)
to come online before we start the tests.
"""
from src.utils.service_utils import wait_for_dependencies

if __name__ == '__main__':
    wait_for_dependencies()
