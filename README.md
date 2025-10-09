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