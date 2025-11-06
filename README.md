# DWD.de NWP ICON datasets downloader

A script to download [DWD](https://www.dwd.de) NWP ICON datasets.

- Website https://www.dwd.de/EN/ourservices/nwp_forecast_data/nwp_forecast_data.html
- Data sources https://opendata.dwd.de/weather/nwp/

See [config.yaml](./config.yaml) for an example configuration.

## CLI

Run with `dwd-downloader [--config ./config.yaml] [--date 20251008]`

By default it will try to incrementally download the most recent datasets available.

## API

```python

from .api import dwd_downloader

dwd_downloader()

# dwd_downloader("./config.yaml")
# dwd_downloader("./config.yaml", "20251008")

```

## Environment variables

- Use `CONFIG_PATH` to specify the config yaml location. 
- Use `LOG_LEVEL` to tune the logging level

`config.yaml` can use env variables replacements

To configure S3 based storage you can provide the following (with `AWS_*` or `S3_*` prefix)

```sh
STORAGE_TYPE=s3
AWS_ACCESS_KEY_ID="minio"
AWS_SECRET_ACCESS_KEY="minio123"
AWS_DEFAULT_REGION="us-east-1"
AWS_ENDPOINT_URL=http://localhost:19000
AWS_BUCKET=local-data
```