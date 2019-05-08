#!/bin/sh

set -e

export WORKSPACE_URL=https://ci.kbase.us/services/ws

flake8 /app
mypy --ignore-missing-imports /app
bandit -r /app
python -m unittest discover /app/test/
