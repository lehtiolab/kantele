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

echo Stopping storage workers
ansible-playbook -i default_inventory -i "${INVENTORY_PATH}" --extra-vars "storage_connect_user=${STORAGE_USER}" storage_stop.yml

echo Stopping analysis workers
ansible-playbook -i default_inventory -i "${INVENTORY_PATH}" --extra-vars "analysis_connect_user=${ANALYSIS_USER}" analysis_stop.yml -K

echo Update storage code
ansible-playbook -i default_inventory -i "${INVENTORY_PATH}" --extra-vars "storage_connect_user=${STORAGE_USER}" storage_update.yml

echo Updating analysis code
ansible-playbook -i default_inventory -i "${INVENTORY_PATH}" --extra-vars "analysis_connect_user=${ANALYSIS_USER}" analysis_update.yml -K

echo Update web node and restart it
ansible-playbook -i default_inventory -i "${INVENTORY_PATH}" --extra-vars "web_connect_user=${WEB_USER}" web-deploy.yml -K

echo Start storage
ansible-playbook -i default_inventory -i "${INVENTORY_PATH}" --extra-vars "storage_connect_user=${STORAGE_USER}" storage_start.yml

echo Start analysis
ansible-playbook -i default_inventory -i "${INVENTORY_PATH}" --extra-vars "analysis_connect_user=${ANALYSIS_USER}" analysis_start.yml -K
