#!/bin/sh

set -e

flake8 /app
mypy --ignore-missing-imports /app
bandit -r /app
python -m src.index_runner.main &
python -m unittest discover /app/src/test/
