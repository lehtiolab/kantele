set -eo pipefail

DOCKERCMD="docker compose --env-file  src/docker/.compose.testing.env -f src/docker/docker-compose-testing.yml"

# remove old test results if needed (locally)
echo Cleaning up
git clean -xf data/teststorage
git checkout -- data/teststorage

# Clean old containers
$DOCKERCMD down

export GROUP_ID=$(id -g)
export USER_ID=$(id -u)

# Lint seems to operate on the local dir
echo Running linting
$DOCKERCMD run web pylint -E --disable E1101,E0307 --ignore-paths '.*\/migrations\/[0-9]+.*.py' \
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
# Get DB container ready so web doesnt try to connect before it has init'ed
$DOCKERCMD up --detach db mq
echo Created db container and started it
sleep 5

echo Init fixture repo
if [ ! -e data/test/nfrepo/.git ]
then
	cd data/test/nfrepo
	git init
	git add *.py
	git commit -m 'test fixtures'
	cd ../../../
fi

echo Running tests
# Run tests

TESTCMD="python manage.py test"
if [[ -z "$1" ]]
then
    $DOCKERCMD run --use-aliases web $TESTCMD home.tests.TestRefineMzmls.test_refine_mzml_move_dbfile --exclude-tag mstulos || ($DOCKERCMD logs web storage_mvfiles storage_downloads tulos_ingester storage_web_rsync && exit 1)
    #$DOCKERCMD run --use-aliases web $TESTCMD mstulos || ($DOCKERCMD logs web storage_mvfiles storage_downloads tulos_ingester && exit 1)
else
    $DOCKERCMD run --use-aliases web $TESTCMD $1|| ($DOCKERCMD logs web storage_mvfiles storage_web_rsync storage_downloads tulos_ingester && exit 1)
fi
$DOCKERCMD down

rm -rf data/test/nfrepo/.git
