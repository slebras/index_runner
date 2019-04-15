#!/bin/sh

set -e

flake8 --max-complexity 7 /app
mypy --ignore-missing-imports /app
bandit -r /app
python -m unittest discover /app/test/
