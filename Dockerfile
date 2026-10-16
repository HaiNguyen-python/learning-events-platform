# Pinned to Debian bookworm: plain "-slim" tags moved to trixie and broke an earlier build.
FROM python:3.12-slim-bookworm

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
COPY contracts ./contracts
COPY dbt ./dbt
# Editable install: contracts/ and dbt/ are found relative to the source tree.
RUN pip install --no-cache-dir -e ".[postgres,report]"

ENV LEVENTS_WAREHOUSE=/app/data/warehouse.duckdb
EXPOSE 8000
HEALTHCHECK CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"
CMD ["sh", "-c", "levents serve --host 0.0.0.0 --port 8000"]
