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

ansible-playbook -i default_inventory -i "${INVENTORY_PATH}" --extra-vars "storage_connect_user=${ANALYSIS_USER}" storage_deploy.yml -K

echo Finished setting up storage server - please make sure that you have set the Tivoli DSMC backup client password,
echo which is a one-time procedure and has to be received from the backup server administrator - if a new server
echo or password is needed, after receiving the password run (manually) "DSM_DIR={{dsm_dir}} dsmc" and it will ask for the new password that first time
