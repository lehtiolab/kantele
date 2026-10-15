#!/bin/bash

# exit as soon as possible
set -eu

# Updates code/containers on entire system
# You will have to stop analyses and other long running tasks by yourself

if [[ ! $(git rev-parse --show-prefix) = 'deploy/' ]]
then
    echo You are not in the git repo deploy folder, exiting
    exit 1
fi

echo Preparing ssh-agent with key
eval $(ssh-agent)
ssh-add

python3 -m venv .venv-ansible
source .venv-ansible/bin/activate
pip install "ansible >2.9"

source .ansible-env

echo Install new certificates on the web node
ansible-playbook -i default_inventory -i "${INVENTORY_PATH}" --extra-vars "web_connect_user=${WEB_USER}" web-certificate.yml -K -vvv
