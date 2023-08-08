#!/bin/bash

set -eu

if [ ! -d "venv" ]; then
    python3 -m venv venv
fi

# shellcheck source=./venv/bin/activate
source ./venv/bin/activate
python -m pip install --upgrade pip
python -m pip install wheel
python -m pip install -r ./requirements.txt
