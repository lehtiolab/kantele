# Kantele

## Development, before starting
Prerequisites:

- docker
- npm 18

First you build the containers for running the Django backend with postgres and nginx:
```
# Create an .env file:
cp .env.example .env

# Edit the new .env file!

# Build containers, with static files and database skeleton
bash src/docker/init_develop.sh

# Now you can run and go to http://${your-machine-url}/
# The admin login / password is test / test
docker compose up
```

## Development:

The backend Django code is mounted in the running web container while developing.
If you change the code, restart with:
```
docker compose restart web
```

In production, the frontend JS is built and stored inside the nginx container, but in development
you do the frontend building yourself, so you can quickly rebuild a frontend app when changing it.
To change a single app (datasets, home, analysis, etc), go to the folder and build:
```
cd src/frontend/datasets
npm run build

# You can run continuous frontend rebuilding with live reload like this:
npm run dev

# For refreshing ALL frontend apps at the same time
# Also does Django static file collection
bash src/docker/create_static.sh
```

If you want to rebuild the containers:
```
# Set user and group id for the file ownership to the same ids the user has.
# I am not sure how to do this on windows or if it is necessary
export USER_ID=$(id -u)
export GROUP_ID=$(id -g)
docker compose build
```

## DB content

Kantele uses a postgres database, and the development container has local folders
bind-mounted in the container for persistence, so if the container is removed, the
database will persist. There is also a backup folder mounted (defined in .env
as `BACKUP_PATH`), through which you can deliver SQL dump files.

Creating a database dump:
```
# If you have the containers up:
docker compose exec db pg_dump -U kanteleuser -d kantele -f /pgbackups/file_to_dump_to.sql

# If the containers are down:
docker compose run db pg_dump -U kanteleuser -d kantele -f /pgbackups/file_to_dump_to.sql
```

Installing a new database dump:
```
# First delete the old DB - if there is none, first start the containers 
# with "docker compose up" to initialize postgres:
# If the containers are down:
docker compose run db dropdb -U postgres kantele
docker compose run db createdb -U postgres kantele
# Now install the new data
docker compose run db psql -U postgres -d kantele -f /pgbackups/file_with_dump.sql

# If instead you have the containers up:
docker compose exec db dropdb -U postgres kantele
docker compose exec db createdb -U postgres kantele
docker compose exec db psql -U postgres -d kantele -f /pgbackups/file_with_dump.sql
```

Upgrading postgres major version, first set the old and new version in `src/docker/postgres-update-compose.yml`,
adjust the paths to the data dir in those (likely different in production),
then do the following (first shutdown application to prevent writes):
```
# Backup data:
mv path/to/data/db path/to/data/olddb
# Start postgres container with old version:
docker compose -f src/docker/postgres-update-compose.yml --env-file .env up olddb
# Create a dump:
docker compose -f src/docker/postgres-update-compose.yml --env-file .env exec olddb pg_dumpall -U postgres > dumpold.sql
# Stop old container, inspect dump file, then start new:
docker compose -f src/docker/postgres-update-compose.yml --env-file .env up newdb
# Load old dump in new container (`-T` is to disable tty so we can pipe the local dump file:
docker compose -f src/docker/postgres-update-compose.yml --env-file .env exec -T newdb psql -U postgres -d postgres < dumpold.sql    

When going from postgres 14 to 15 or higher you will also need to add:
docker compose -f src/docker/postgres-update-compose.yml --env-file .env exec -T db psql -U postgres -d kantele
GRANT ALL ON SCHEMA public TO kanteleuser; 
# Somehow in testing my migrations after this crash with "no permission" until I do it again
# in the same compose environment as the migrations, which is weird since the web UI runs fine
```


## Testing

To run the Django tests (python unittests):
```
# Run all the tests:
bash run_tests.sh

# Or run a specific test
bash run_tests.sh analysis.tests.TestGetAnalysis.test_ok
```

Tests run in docker compose containers and are also running on github actions inside the same containers.


## Repo
There is a `data` directory for persistent data between container rebuilds in
development and testing. The directory contains:
- `analysisfiles` - analysis results go here, currently only work in prod
- `db` - database persistence
- `test` - contain filesystem fixtures for tests
- `teststorage` - empty, gitignored, used as a fresh fixture workdir in testing, populated by shutil.copytree in base test
- `storage` - used in develop/stage as a storage unit
- `newstorage` - for simulating a second storage unit
- `uploads` - where uploads go in the nginx container
