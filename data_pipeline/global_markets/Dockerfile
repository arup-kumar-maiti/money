FROM python:3.14.7-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY data_pipeline/global_markets/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY data_pipeline/global_markets/*.py data_pipeline/global_markets/config.toml ./global_markets/
COPY data_pipeline/global_markets/bigquery/*.sql ./global_markets/bigquery/
COPY shared/__init__.py shared/config.py shared/delta.py ./shared/

USER 10001:10001

ENTRYPOINT ["python", "-m", "global_markets"]
