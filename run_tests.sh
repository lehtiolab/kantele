set -eo pipefail

DOCKERCMD="docker compose --env-file  src/docker/.compose.testing.env -f src/docker/docker-compose-testing.yml --project-name kanteletest"

# remove old test results if needed (locally)
echo Cleaning up
git clean -xf data/teststorage
git checkout -- data/teststorage

# Make sure that clean containers are being used
# No other containers running.
$DOCKERCMD down --remove-orphans

if [ "$(uname)" = "Darwin" ]; then
  export USER_ID=1000
  export GROUP_ID=1000
else
  export USER_ID=$(id -u)
  export GROUP_ID=$(id -g)
fi

# Lint seems to operate on the local dir
echo Running linting
$DOCKERCMD run -T web pylint -E --disable E1101,E0307 --ignore-paths '.*\/migrations\/[0-9]+.*.py' \
   analysis \
   datasets \
   dashboard \
   home \
   jobs \
   kantele \
   rawstatus \
   || (echo Linting failed && $DOCKERCMD down && exit 2)
echo Linting OK

echo Prebuilding DB and MQ containers
$DOCKERCMD up --detach --wait db mq
echo db and mq ready


echo Init fixture repos
if [ ! -e data/test/nfrepo/.git ]
then
	cd data/test/nfrepo
	git -c init.defaultBranch=master init # Make sure that you use the same git branch naming conventions
	git add *.py
	git commit -m 'test fixtures'
	cd ../../../
fi

echo Running tests
# Run tests

TESTCMD="python manage.py test"
if [[ -z "$1" ]]
then
    $DOCKERCMD run --use-aliases web $TESTCMD --tag slow --exclude-tag mstulos --exclude-tag home
    $DOCKERCMD run --use-aliases web $TESTCMD --tag slow --exclude-tag mstulos --exclude-tag analysis --exclude-tag datasets --exclude-tag storage
    $DOCKERCMD run --use-aliases web $TESTCMD --exclude-tag slow --exclude-tag mstulos
else
    $DOCKERCMD run --use-aliases web $TESTCMD $1
fi
echo Remove Git nfrepo
rm -rf data/test/nfrepo/.git
