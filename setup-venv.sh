#!/bin/bash

set -eu

this_script="${BASH_SOURCE[0]}"
this_script_dir="$(dirname "$this_script")"


if [ ! -d "venv" ]; then
    python3 -m venv venv
fi

# cf. https://www.shellcheck.net/wiki/SC1091
# shellcheck source=/dev/null
source ./venv/bin/activate
python -m pip install --upgrade pip
python -m pip install --upgrade digiflow
python -m pip install -r "$this_script_dir"/requirements.txt
