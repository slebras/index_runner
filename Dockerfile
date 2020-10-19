FROM python:3.7-alpine

# Dockerize related args
ARG BUILD_DATE
ARG VCS_REF
ARG BRANCH=develop
ENV DOCKERIZE_VERSION v0.6.1

# Install dockerize
RUN apk --update add --virtual deps curl tar gzip && \
    curl -o dockerize.tar.gz https://raw.githubusercontent.com/kbase/dockerize/master/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz && \
    tar -C /usr/local/bin -xvzf dockerize.tar.gz && \
    rm dockerize.tar.gz && \
    apk del deps

# Dockerize related labels
LABEL org.label-schema.build-date=$BUILD_DATE \
      org.label-schema.vcs-url="https://github.com/kbase/index_runner" \
      org.label-schema.vcs-ref=$VCS_REF \
      org.label-schema.schema-version="1.0.0-rc1" \
      us.kbase.vcs-branch=$BRANCH \
      maintainer="KBase Team"

WORKDIR /app

# Dependency for confluent-kafka
RUN apk --update add librdkafka librdkafka-dev && ldconfig /usr/lib

# Install dependencies
COPY pyproject.toml poetry.lock /app/
RUN apk --update add --virtual deps python3-dev build-base libffi-dev libressl-dev && \
    pip install --upgrade pip poetry==1.0.9 && \
    poetry config virtualenvs.create false && \
    poetry install --no-dev --no-interaction --no-ansi && \
    apk del deps

# Make the admin tools executable
RUN ln -s /app/src/admin_tools/indexer_admin /usr/local/bin/indexer_admin

COPY . /app

ENTRYPOINT ["/usr/local/bin/dockerize"]
CMD ["/app/scripts/docker/start_server"]
