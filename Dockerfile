FROM python:3.7-slim

ARG DEVELOPMENT
ARG BUILD_DATE
ARG VCS_REF
ARG BRANCH=develop

# Install dockerize
RUN apt-get update && \
    apt-get install -y wget
ENV DOCKERIZE_VERSION v0.6.1
RUN wget https://github.com/kbase/dockerize/raw/master/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz && \
    tar -C /usr/local/bin -xvzf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz && \
    rm dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz

# Install pip requirements
COPY requirements.txt dev-requirements.txt /tmp/
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt && \
    if [ "$DEVELOPMENT" ]; then pip install --no-cache-dir -r /tmp/dev-requirements.txt; fi

COPY src /app
COPY src/scripts /app/scripts

WORKDIR /app
ENV KB_DEPLOYMENT_CONFIG=/app/deploy.cfg

LABEL org.label-schema.build-date=$BUILD_DATE \
      org.label-schema.vcs-url="https://github.com/kbase/relation_engine_api" \
      org.label-schema.vcs-ref=$VCS_REF \
      org.label-schema.schema-version="1.0.0-rc1" \
      us.kbase.vcs-branch=$BRANCH \
      maintainer="KBase Team"

# Make the admin tools executable
RUN ln -s /app/admin_tools/indexer_admin /usr/local/bin/indexer_admin

ENTRYPOINT ["/usr/local/bin/dockerize"]
CMD ["python", "-m", "index_runner.main"]
