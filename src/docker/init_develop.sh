#!/bin/bash
#
# This script is to create a fresh installation for developing in
# Not to be used in production settings

# exit as soon as possible
set -eu

# Updates code/containers on entire system
# You will have to stop analyses and other long running tasks by yourself

if [[ ! $(git rev-parse --show-prefix) = '' ]]
then
    echo You are not in the git repo base folder, exiting
    exit 1
fi

# Set user and group id for the file ownership to the same ids the user has.
# I am not sure how to do this on windows or if it is necessary

if [ "$(uname)" = "Darwin" ]; then
  export USER_ID=1000
  export GROUP_ID=1000
else
  export USER_ID=$(id -u)
  export GROUP_ID=$(id -g)
fi

# Now build the containers
docker compose build

# Create static files
bash src/docker/create_static.sh

# Init database (sleep is to let DB container come up and init)
# First regular migrations (auth, admin, etc)
docker compose run -e CLEAN_DB_INIT=1 web bash -c "sleep 5 && python manage.py migrate"
# Now fresh non-migrating migrations
docker compose run -e CLEAN_DB_INIT=1 web python manage.py migrate --run-syncdb
# Mark migrations as solved (fake)
docker compose run web python manage.py migrate --fake
# Create a super user
docker compose run -e DJANGO_SUPERUSER_PASSWORD=test web python manage.py createsuperuser --username test --email "test@example.com" --noinput
