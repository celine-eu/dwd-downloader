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

## Variables

### 🌞 Radiation and Energy Fluxes

- alb_rad – Surface albedo
- asob_s – Net shortwave radiation at surface (all-sky)
- asob_s_cs – Net shortwave radiation at surface (clear-sky)
- asob_t – Net shortwave radiation at top of atmosphere
- aswdifd_s – Diffuse downward shortwave radiation at surface
- aswdifu_s – Diffuse upward shortwave radiation at surface
- aswdir_s – Direct shortwave radiation at surface
- athb_s – Net longwave radiation at surface (all-sky)
- athb_t – Net longwave radiation at top of atmosphere
- alhfl_s – Latent heat flux at surface
- ashfl_s – Sensible heat flux at surface
- apab_s – Absorbed shortwave flux at surface

### ☁️ Cloud and Precipitation

- clc – Cloud cover (3D field)
- clch – High cloud cover
- clcm – Medium cloud cover
- clcl – Low cloud cover
- clct – Total cloud cover
- clct_mod – Model-diagnostic total cloud cover
- cldepth – Cloud depth
- htop_con – Convective cloud top height
- hbas_con – Convective cloud base height
- htop_dc – Top of deep convection
- ceiling – Cloud base (ceiling) height
- hzerocl – Height of the 0°C isotherm (freezing level)
- rain_con – Convective rain rate
- rain_gsp – Large-scale (stratiform) rain rate
- snow_con – Convective snow rate
- snow_gsp – Stratiform snow rate
- tot_prec – Total precipitation (rain + snow)
- snowlmt – Snow line altitude
- rho_snow – Density of snow
- h_snow – Snow depth
- w_snow – Water equivalent of snow

### 🌬️ Dynamics (Wind, Motion, Pressure)

- u – Zonal wind component
- v – Meridional wind component
- u_10m – 10-meter zonal wind
- v_10m – 10-meter meridional wind
- vmax_10m – Maximum 10-meter wind gust
- w – Vertical velocity
- omega – Vertical velocity in pressure coordinates
- tke – Turbulent kinetic energy
- p – Pressure (3D field)
- ps – Surface pressure
- pmsl – Mean sea level pressure

### 🌡️ Temperature and Humidity

- t – Air temperature (3D field)
- t_2m – 2-meter air temperature
- tmax_2m – Maximum 2-meter temperature
- tmin_2m – Minimum 2-meter temperature
- td_2m – 2-meter dew point temperature
- t_g – Ground (skin) temperature
- t_snow – Snow temperature
- t_ice – Ice temperature
- t_so – Soil temperature (by layer)
- qv – Specific humidity (3D field)
- qv_2m – 2-meter specific humidity
- relhum – Relative humidity (3D field)
- relhum_2m – 2-meter relative humidity

### 🌍 Surface and Soil

- hsurf – Surface altitude (orography)
- z0 – Surface roughness length
- soiltyp – Soil type (categorical)
- lai – Leaf area index
- rootdp – Root depth
- w_so – Soil water content
- w_so_ice – Frozen soil water content
- runoff_s – Surface runoff
- runoff_g – Groundwater runoff

### ❄️ Ice, Lakes, and Land Fractions

- fr_land – Fraction of land in grid cell
- fr_lake – Fraction of lake in grid cell
- depth_lk – Lake depth
- h_ice – Ice thickness

### ⚡ Convection and Stability

- cape_ml – Convective available potential energy (mixed layer)
- cape_con – CAPE for convective updrafts
- cin_ml – Convective inhibition (mixed layer)
- lpi_con_max – Lightning potential index
- tch – Temperature at convective cloud top
- tcm – Temperature at convective cloud middle
- mh – Height of mixed layer

### 🌫️ Optical and Remote Sensing

- vis – Visibility
- synmsg_bt_cl_ir10.8 – Synthetic brightness temperature (IR 10.8 µm)
- synmsg_bt_cl_wv6.2 – Synthetic brightness temperature (WV 6.2 µm)

### 💧 Moisture and Condensate (3D and Integrated)

- qc – Cloud water content
- qi – Cloud ice content
- qr – Rain water content
- qs – Snow content
- qv_s – Specific humidity at surface
- tqc – Total column cloud water
- tqi – Total column cloud ice
- tqr – Total column rain water
- tqs – Total column snow
- tqv – Total column water vapor

### 📈 Geometry and Coordinates

- rlat – Rotated latitude coordinate
- rlon – Rotated longitude coordinate
- hhl – Height of model half levels (vertical grid)
- fi – Geopotential
- plcov – Plant cover fraction