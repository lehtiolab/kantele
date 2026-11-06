# Deploying Kantele

## Deploy production using bash scripts:

```
# When only web server gets updates (frontend, API, etc):
bash deploy_web.sh

# When analysis or storage workers change:
bash deploy_analysis.sh
bash deploy_hpc.sh
bash deploy_storage.sh

# When there are changes in all components:
bash deploy_system.sh
```

## Docker

Kantele is run using `docker compose`, and the configuration is a bit finicky. We have separate YML
compose files for testing and production, and these contain a number of variables which are filled
in through environment variables passed to docker like so: `--env-file .compose.env`. Production 
is deployed using `ansible`, and this means the `.compose.env` files are populated using ansible
variables. Some of the ansible variables are templated directly into the `docker-compose-prod.yml`,
but those shared with e.g. `docker-compose-base.yml` will need to take a detour via the env file.
