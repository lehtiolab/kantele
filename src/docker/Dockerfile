# Django app
FROM python:3.13.18 AS django
ARG USER_ID
ARG GROUP_ID
RUN addgroup --system --gid $GROUP_ID kantele
RUN adduser --system --uid $USER_ID --gid $GROUP_ID kantele
RUN apt-get update && apt-get install -y libpq-dev python3-psycopg2 zip
COPY backend/requirements.txt /
RUN pip install -r requirements.txt
RUN mkdir /assets
RUN chdir /assets && wget https://files.pythonhosted.org/packages/a0/f4/c67b0b3f1b9245e8d266f0f112c500d50e5b4e83cb6f3b71b6528104182a/requests-2.34.2-py3-none-any.whl
RUN chdir /assets && wget https://files.pythonhosted.org/packages/32/d1/a3652fe2c19134d0d15922177821d863cbd7e842d53a21bbe7e8e3a1b5ac/certifi-2026.10.23-py3-none-any.whl
RUN chdir /assets && wget https://files.pythonhosted.org/packages/3f/51/d4db610ef29373b879047326cbf6fa98b6c1969d6f6dc423279de2b1be2c/requests_toolbelt-1.0.0-py2.py3-none-any.whl
RUN chdir /assets && wget https://files.pythonhosted.org/packages/58/a2/bb081bab032533a855d44de1d56f8e8426114ff1ba5d1f07a438a0a654f8/idna-3.20-py3-none-any.whl
RUN chdir /assets && wget https://files.pythonhosted.org/packages/fc/ad/d07d7862a62ffa6d79d68074d14823243dd235a77c45262acbf6adeb28bf/charset_normalizer-3.5.2-py3-none-any.whl
RUN chdir /assets && wget https://files.pythonhosted.org/packages/92/9d/c4e665119135114480843e7ab388fa94d8480650450e6f8e26b70d323a4c/urllib3-2.8.0-py3-none-any.whl
RUN chdir /assets && wget https://files.pythonhosted.org/packages/b4/90/e2159492b5426be0c1fef7acba807a03511f97c5f86b3caeda6ad92351a7/psutil-7.2.2-cp37-abi3-win_amd64.whl
COPY docker/setup.bat /assets/
RUN cd /assets && zip producer.zip *.whl setup.bat && chown kantele:kantele producer.zip
USER kantele
COPY --chown=kantele backend /kantele
WORKDIR /kantele
CMD uwsgi --socket :8009 --processes 3 --module kantele.wsgi

# For storage container (use in testing)
FROM django AS django_test
USER root
RUN apt-get update && apt-get install -y rsync
RUN pip install pylint
RUN mkdir /kantelessh
COPY docker/test_rsync_sshkey /kantelessh/rsync_key
RUN chmod 700 /kantelessh && chown -R kantele: /kantelessh && chmod 400 /kantelessh/rsync_key
USER kantele

# Build django static for the nginx prod container
FROM django AS django_static
ARG SECRET_KEY=abcd1234
RUN python manage.py collectstatic --no-input

# Compile node frontend stuff for prod container
FROM node:24 AS build_frontend
COPY frontend /src/frontend
RUN cd /src/frontend/analysis && npm install && npm run build
RUN cd /src/frontend/dashboard && npm install && npm run build
RUN cd /src/frontend/datasets && npm install && npm run build
RUN cd /src/frontend/file-inflow && npm install && npm run build
RUN cd /src/frontend/home && npm install && npm run build
RUN cd /src/frontend/mstulos && npm install && npm run build
RUN cd /src/frontend/staffpage && npm install && npm run build
RUN cd /src/frontend/corefac && npm install && npm run build

# Nginx to serve static stuff, uploads etc
# also copy in the relevant config files
FROM nginx:1.27.5 AS nginx_prod
COPY --from=build_frontend /static /static
COPY --from=django_static /kantele/static/ /static/
COPY docker/nginx.conf /etc/nginx/nginx.conf
COPY docker/uwsgi_params /etc/nginx/uwsgi_params


# Rsync SSH host for testing (SSHD would run on the web host machine outside of container
# in normal circumstances)
# Ubuntu 24 has already ubuntu/ubuntu user with 1000/1000, so sticking with 22 for now.
FROM ubuntu:24.04 AS rsync_storage
RUN apt-get update && apt-get install -y openssh-server rsync
ARG USER_ID
ARG GROUP_ID
RUN groupmod -g $GROUP_ID -n kantele ubuntu
RUN usermod -ml kantele -u $USER_ID -d /home/kantele ubuntu
RUN mkdir /home/kantele/.ssh && chown kantele:kantele /home/kantele/.ssh && chmod 700 /home/kantele/.ssh
RUN mkdir -p /root/.ssh && chmod 700 /root/.ssh
CMD service ssh start -D


# Analysis if needed
FROM django AS nextflow
USER root
RUN apt update && apt install -y git-core
USER kantele
