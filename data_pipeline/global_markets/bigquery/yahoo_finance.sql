CREATE OR REPLACE EXTERNAL TABLE
`dev-workspace-12345.global_markets.yahoo_finance`
WITH CONNECTION `dev-workspace-12345.us-central1.external-data`
OPTIONS (
    format = 'DELTA_LAKE',
    uris = ['gs://money-dev-12345/data_pipeline/global_markets/bronze/yahoo_finance']
);
