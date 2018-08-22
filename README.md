# OSMGeocoder

Python implementation for a OSM Geocoder
(Only works on Python 3 for now)

## TODO

- Do not run geocoder on buildings only, probably roads and distances are possible

## Quick and dirty how-to

1. Create a PostgreSQL Database with PostGIS activated
2. Fetch a copy of [imposm3](https://github.com/omniscale/imposm3)
3. Get a OpenStreetMap data file (for example from [Geofabrik](http://download.geofabrik.de/), start with a small region!)
4. Import some OpenStreetMap data into the DB:
```bash
$ imposm import -connection postgis://user:password@host:port/database -mapping doc/imposm_mapping.yml -read /path/to/osm.pbf -write -deployproduction -optimize
```
5. Create the trigram and fuzzy string search extension for the DB:
```sql
CREATE EXTENSION pg_trgm;
CREATE EXTENSION fuzzystrmatch;
```
6. Create a trigram search indices and text prediction wordlists (this could take a while):
```bash
psql osm < doc/create_trigram_indices.sql
psql osm < doc/predict_text.sql
```
7. Create a virtualenv and install packages:
```bash
mkvirtualenv -p /usr/bin/python3 osmgeocoder
workon osmgeocoder
pip install -r requirements.txt
```
8. Modify configuration file to match your setup. The example config is in `config/config.json`.
9. Optionally install and start the postal machine learning address categorizer (see below)
10. Geocode:
```bash
bin/address2coordinate.py --config config/config.json --center 48.3849 10.8631 Lauterl
bin/coordinate2address.py --config config/config.json 48.3849 10.8631
```

**NOTE:** you can also install this via pip:
- the scripts from the `bin` directory will be copied to your environment.
- the SQL files will be placed in your virtualenv in `share/osmgeocoder/sql`
- the YAML files will be placed in your virtualenv in `share/osmgeocoder/yml`
- An example config file will be placed in your virtualenv in `share/doc/osmgeocoder/config-example.json`
- The PIP installation will not install `flask` and `gunicorn` nor will it try to install `postal`,
  if you want to use those services you need to install those optional dependencies yourself (read on!)


## Optional support for libpostal

### Installation of libpostal

Be aware that the make process will download some data-files (about 1GB in size). The installation of libpostal
will need around 1 GB of disk space and about 2 GB of disk space while compiling.

Currently there is no Ubuntu package for `libpostal`, so we have to install it by hand:

```bash
git clone https://github.com/openvenues/libpostal
cd libpostal
./bootstrap.sh
./configure --prefix=/opt/libpostal --datadir=/opt/libpostal/share
make -j4
sudo make install
echo "/opt/libpostal/lib" | sudo tee /etc/ld.so.conf.d/libpostal.conf
sudo ldconfig
echo 'export PKG_CONFIG_PATH="$PKG_CONFIG_PATH:/opt/libpostal/lib/pkgconfig"' | sudo tee /etc/profile.d/libpostal.sh
```

Now log out and on again or run a new login shell (e.g. `bash -l`) and install the missing python modules:

```bash
workon osmgeocoder
CFLAGS="-L/opt/libpostal/lib -I/opt/libpostal/include" pip install postal
pip install gunicorn
pip install flask
```

### Run the classifier service

**Source checkout:**

```bash
workon osmgeocoder
bin/postal_service.py --config config/config.json
```

**PIP install:**

```bash
/path/to/virtualenv/bin/postal_service.py --config config.json
```

Attention: Depending on the speed of your disk, startup of this service may take some seconds
(this is why this is implemented as a service) and it will take about 2 GB of RAM, so be warned!


If you want to run it in production mode just run it with `gunicorn` directly.
See the [Gunicorn documentation](http://docs.gunicorn.org/en/latest/settings.html) for further information.
Simple Example is following (one worker, run as daemon, bind to 127.0.0.1:3200):

```bash
workon osmgeocoder
gunicorn postal_service:app \
    --bind 127.0.0.1:3200 \
    --workers 1 \
    --pid /var/run/postal_service.pid \
    --log-file /var/log/postal_service.log \
    --daemon
```

## Running a HTTP geocoding service

The file `geocoder_service.py` is a simple Flask app to present the geocoder as a HTTP service.

### Installation

```bash
workon osmgeocoder
pip install gunicorn
pip install flask
```

You will need a working config file too.

### Run the service

The service will search for a config file in the following places:
- `~/.osmgeocoderrc`
- `~/.config/osmgeocoder.json`
- `/etc/osmgeocoder.json`
- `osmgeocoder.json`

You can override the path by setting the environment variable `GEOCODER_CONFIG`.

Gunicorn example:

```bash
workon osmgeocoder
gunicorn geocoder_service:app \
    --env 'GEOCODER_CONFIG=config/config.json'
    --bind 127.0.0.1:8080 \
    --workers 4 \
    --pid /var/run/osmgeocoder_service.pid \
    --log-file /var/log/osmgeocoder_service.log \
    --daemon
```

### Defined API-Endpoints

#### Forward geocoding

Address string to coordinate.

- Endpoint `/forward`
- Method `POST`
- Content-Type `application/json`
- Body:
    - `address`: (required) User input / address to convert to coordinates
    - `center`: (optional) Array with center coordinate to sort matches
    - `country`: (optional) ISO Country code, use only if no center coordinate is available
- Response: Array of objects
    - `address`: Fully written address line, formatted by country standards
    - `lat`: Latitude
    - `lon`: Longitude

#### Reverse geocoding

Coordinate to address string.

- Endpoint `/reverse`
- Method `POST`
- Content-Type `application/json`
- Body:
    - `lat`: Latitude
    - `lon`: Longitude
- Response: Object
    - `address`: Nearest address to the point (building search) or `null`, formatted by country standards

#### Predictive text

Intelligent text completion while typing.

- Endpoint `/predict`
- Method `POST`
- Content-Type `application/json`
- Body:
    - `query`: User input
- Response: Object
    - `predictions`: Up to 10 text predictions, sorted by equality and most common first


## Config file

Example:

```json
{
  "db": {
    "dbname": "osm",
    "user": "johannes",
    "password": "design"
  },
  "tables":{
    "buildings": "osm_buildings",
    "roads": "osm_roads",
    "postcode": "osm_postal_code",
    "admin": "osm_admin"
  },
  "opencage_data_file": "doc/worldwide.yml",
  "postal_service_url": "http://localhost:3200/",
  "postal_service_port": 3200
}
```

Keys:

- `db`: Database configuration this will be built into a [Postgres connection string](https://www.postgresql.org/docs/current/static/libpq-connect.html#id-1.7.3.8.3.5)
- `tables`: Table names to use, if you use the supplied imposm mapping you can just use the values from the example
- `postal_service_url`: (optional) URL where to find the libpostal service, if not supplied searching is reduced to street names only
- `postal_service_port`: (optional) only used when running the libpostal service directly without explicitly using gunicorn
- `opencage_data_file`: (optional) Data file for the address formatter, defaults to the one included in the package

## API documentation

The complete project contains actually only two classes:

### `Geocoder`.

Publicly accessible method prototypes are:

```python
def __init__(self, config):
    pass

def forward(self, address, country=None, center=None):
    pass

def reverse(self, lat, lon, limit=10):
    pass

def predict_text(self, input):
    pass
```

#### `__init__`

Initialize a geocoder, this will read all files to be used and set up the DB connection.
- `config`: Dictionary with configuration values, see __Config File__ above for used keys.

#### `forward`

Geocode an address to a lat, lon location.
- `address`: Address to code
- `country`: (optional) Country code to restrict search and format address
- `center`: (optional) Center coordinate to sort results for (will be used to determine country too, so you can skip the `country` flag)

This function is a generator which `yield`s the obtained results.

#### `reverse`

Geocode a lat, lon location into a readable address:
- `lat`: Latitude to code
- `lon`: Longitute to code
- `limit`: (optional) maximum number of results to return

This function is a generator which `yield`s the obtained results.

#### `predict_text`

Return possible text prediction results for the user input. This could be used while the user is typing their query to reduce the load on the database (by avoiding typos and running fewer requests against the geocoder because the user skips over typing long words one character by each).
- `input`: User input

This function is a generator which `yield`s the obtained results.

**ATTENTION**: Do not feed complete "sentences" into this function as it will not yield the expected result, tokenize into words on client side and only request predictions for the current word the user is editing.


### `AddressFormatter`

Publicly accessible method prototypes are:

```python
def __init__(self, config=None):
    pass

def format(self, address, country=None):
    pass
```

#### `__init__`

Initialize the address formatter
- `config`: (optional) override default config file to use for the address formatter, defaults to config file included in this package

#### `format`

Format an address in the default layout used in the specified country. Return value may contain line breaks.
- `address`: Dictionary that contains the address parts, see below for recognized keys
- `country`: Country code of the formatting template to use

Recognized keys in `address`:
- `attention`
- `house`
- `road`
- `house_number`
- `postcode`
- `city`
- `town`
- `village`
- `county`
- `state`
- `country`
- `suburb`
- `city_district`
- `state_district`
- `state_code`
- `neighbourhood`