#!/bin/bash

set -eu

THE_SCRIPT="${BASH_SOURCE[0]}"
THE_DIR="$(dirname "${THE_SCRIPT}")"
THE_PYTHON=python3.10


if [ -d "venv" ]; then
    rm -vr venv
fi
${THE_PYTHON} -m venv venv

# cf. https://www.shellcheck.net/wiki/SC1091
# shellcheck source=/dev/null
source ./venv/bin/activate
python -m pip install --upgrade pip
python -m pip install --upgrade digiflow
python -m pip install -r "${THE_DIR}"/requirements.txt
