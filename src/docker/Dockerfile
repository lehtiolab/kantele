# Django app
FROM python:3.13.3 AS django
ARG USER_ID
ARG GROUP_ID
RUN addgroup --system --gid $GROUP_ID kantele
RUN adduser --system --uid $USER_ID --gid $GROUP_ID kantele
RUN apt-get update && apt-get install -y libpq-dev python3-psycopg2 zip
COPY backend/requirements.txt /
RUN pip install -r requirements.txt
RUN mkdir /assets
RUN chdir /assets && wget https://files.pythonhosted.org/packages/f9/9b/335f9764261e915ed497fcdeb11df5dfd6f7bf257d4a6a2a686d80da4d54/requests-2.32.3-py3-none-any.whl
RUN chdir /assets && wget https://files.pythonhosted.org/packages/38/fc/bce832fd4fd99766c04d1ee0eead6b0ec6486fb100ae5e74c1d91292b982/certifi-2025.1.31-py3-none-any.whl
RUN chdir /assets && wget https://files.pythonhosted.org/packages/3f/51/d4db610ef29373b879047326cbf6fa98b6c1969d6f6dc423279de2b1be2c/requests_toolbelt-1.0.0-py2.py3-none-any.whl
RUN chdir /assets && wget https://files.pythonhosted.org/packages/76/c6/c88e154df9c4e1a2a66ccf0005a88dfb2650c1dffb6f5ce603dfbd452ce3/idna-3.10-py3-none-any.whl
RUN chdir /assets && wget https://files.pythonhosted.org/packages/0e/f6/65ecc6878a89bb1c23a086ea335ad4bf21a588990c3f535a227b9eea9108/charset_normalizer-3.4.1-py3-none-any.whl
RUN chdir /assets && wget https://files.pythonhosted.org/packages/6b/11/cc635220681e93a0183390e26485430ca2c7b5f9d33b15c74c2861cb8091/urllib3-2.4.0-py3-none-any.whl
RUN chdir /assets && wget https://files.pythonhosted.org/packages/50/1b/6921afe68c74868b4c9fa424dad3be35b095e16687989ebbb50ce4fceb7c/psutil-7.0.0-cp37-abi3-win_amd64.whl
COPY docker/setup.bat /assets/
RUN cd /assets && zip producer.zip *.whl setup.bat && chown kantele:kantele producer.zip
USER kantele
COPY --chown=kantele backend /kantele
WORKDIR /kantele
CMD uwsgi --socket :8009 --processes 3 --module kantele.wsgi

# For storage container (use in testing)
FROM django AS django_test
USER root
RUN apt-get install -y rsync
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
FROM node:23 AS build_frontend
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
FROM ubuntu:22.04 AS rsync_storage
RUN apt-get update && apt-get install -y openssh-server rsync
ARG USER_ID
ARG GROUP_ID
RUN addgroup --system --gid $GROUP_ID kantele
RUN adduser --system --uid $USER_ID --gid $GROUP_ID --shell /bin/bash kantele
RUN mkdir /home/kantele/.ssh && chown kantele:kantele /home/kantele/.ssh && chmod 700 /home/kantele/.ssh
RUN mkdir /root/.ssh && chmod 700 /root/.ssh
CMD service ssh start -D


# Analysis if needed
FROM django AS nextflow
USER root
RUN apt install -y openjdk-17-jre
RUN mkdir /nxf && cd /nxf && curl -s https://get.nextflow.io | bash
RUN chown -R kantele:kantele /nxf
USER kantele
