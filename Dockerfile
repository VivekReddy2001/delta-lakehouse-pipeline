FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /app
COPY pyproject.toml README.md ./
COPY lakehouse ./lakehouse
RUN pip install --no-cache-dir .
# Resolve the Delta Lake jars at build time so containers start offline.
RUN python -c "from lakehouse.spark import get_spark; get_spark().stop()"

ENTRYPOINT ["python", "-m", "lakehouse", "--lake", "/data/tables", "--landing", "/data/landing"]
CMD ["run"]
