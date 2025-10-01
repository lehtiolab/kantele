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

source .ansible-env

echo Preparing ssh-agent with key
eval $(ssh-agent)
ssh-add "${LOCAL_SSH_KEY}"
ssh-add "${HPC_SSH_KEY}"

python3 -m venv .venv-ansible
source .venv-ansible/bin/activate
pip install "ansible >2.9"

echo Stopping storage workers
ansible-playbook -i default_inventory -i "${INVENTORY_PATH}" --extra-vars "storage_connect_user=${STORAGE_USER} onlystop=true" storage_deploy.yml -K

echo Stopping analysis workers
ansible-playbook -i default_inventory -i "${INVENTORY_PATH}" --extra-vars "analysis_connect_user=${ANALYSIS_USER} onlystop=true" analysis_deploy.yml -K

echo Update web node and restart it
ansible-playbook -i default_inventory -i "${INVENTORY_PATH}" --extra-vars "web_connect_user=${WEB_USER}" web-deploy.yml -K

echo Update storage code
ansible-playbook -i default_inventory -i "${INVENTORY_PATH}" --extra-vars "storage_connect_user=${STORAGE_USER}" storage_deploy.yml -K

echo Updating analysis code
ansible-playbook -i default_inventory -i "${INVENTORY_PATH}" --extra-vars "analysis_connect_user=${ANALYSIS_USER}" analysis_deploy.yml -K

echo Update HPC analysis code
ansible-playbook -i default_inventory -i "${INVENTORY_PATH}" --extra-vars "analysis_connect_user=${HPC_USER}" hpc_analysis_deploy.yml -K
