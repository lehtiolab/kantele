#!/usr/bin/env bash

set -eo pipefail

if [[ -z "$1" ]]
then
    echo Need to pass a file like bash kantele_upload.sh /path/to/file.fa
    exit 2
fi


echo Inititalizing upload script ...
python3 -m venv .kantele-upload-venv
source .kantele-upload-venv/bin/activate
pip install requests requests_toolbelt psutil

python upload.py --files "$@"

