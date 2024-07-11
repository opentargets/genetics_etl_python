FROM python:3.10-bullseye

ARG CLOUD_SDK_VERSION=452.0.0
ENV GCLOUD_HOME=/home/google-cloud-sdk
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk jq && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV PATH="$PATH:$GCLOUD_HOME"
# required by google batch scripts
RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
    --bash-completion=false \
    --path-update=false \
    --usage-reporting=false \
    --quiet \
    && rm -rf "${TMP_DIR}"

ENV PATH="$PATH:$GCLOUD_HOME/bin"
RUN java -version

# Set environment variables for Java
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

RUN pip install poetry==1.7.1

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

WORKDIR /app

COPY pyproject.toml poetry.lock ./
RUN touch README.md

RUN poetry config installer.max-workers 10
RUN poetry install --without dev,docs,tests --no-root --no-interaction --no-ansi -vvv && rm -rf $POETRY_CACHE_DIR

COPY src ./src

RUN poetry install --without dev,docs,tests

ENTRYPOINT ["poetry", "run", "gentropy"]
