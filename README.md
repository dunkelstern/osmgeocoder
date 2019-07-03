# OSMGeocoder

Python implementation for a OSM / Openaddresses.io Geocoder.

This geocoder is implemented in PostgreSQL DB functions as much as possible, there is a simple API and an example flask app included.

You will need PostgreSQL 9.5+ (or 11.0+ for OpenAddresses.io support) with PostGIS installed as well as some disk space and data-files from OpenStreetMap and (optionally) OpenAddresses.io.

Data import will be done via [Omniscale's imposm3](https://github.com/omniscale/imposm3) and a supplied python script to import the openaddresses.io data.

Optionally you can use the [libpostal machine learning address classifier](https://github.com/openvenues/libpostal) to parse addresses supplied as input to the forward geocoder.

For formatting the addresses from the reverse geocoder the `worldwide.yml` from [OpenCageData address-formatting repository](https://github.com/OpenCageData/address-formatting) is used to format the address according to customs in the country that is been encoded.

See `README.md` in the [repository](https://github.com/dunkelstern/osmgeocoder) for more information.

## Changelog

### v1.0

- Initial release, reverse geocoding works, forward geocoding is slow

### v2.0

**Warning:** DB Format changed, you'll have to re-import data

- Fixed forward geocoding speed
- Fixed import scripts to be more resilient
- Made Openaddresses.io completely optional
- Restored compatability with older 3.x python versions
- Restored compatability with older PostgreSQL DB versions (9.5+ if you do no use openaddresses.io)
- Switched to `pipenv`

## TODO

- Return Attribution in API and in webservices

## "Quick" and dirty how-to

**Statistics uutdated, will be updated shortly**

Just for your information, this process takes a lot of time for a big import. Example figures on a machine with a Core i7-7700K on 4.2 GHz with a Samsung (SATA-)SSD and 32GB of RAM (and some tuned buffer sizes for Postgres):

- Import of the Europe-Region of OpenStreetMap:
    - Import time: 3 hours
    - OSM Data file: 20 GB
    - Temporary space needed: 35 GB
    - Final size in DB: 58.7 GB
    - Summary of space requirement: 115 GB
- Import of the two Openaddresses.io files for Europe:
    - Import time: 1 hour
    - Data files: 4 GB
    - Temporary space needed: 2 GB
    - Final size in DB: 18 GB
    - Summary of space requirement: 24 GB
- Conversion of the OpenStreetMap data into geocoding format:
    - Conversion time: 5 hours
    - Final size in DB: 10.5GB

So in summary you'll need 9 hours of time and 150 GB of disk space.
After cleanup you'll need 28.5 GB of disk space for the Europe data set. A compressed DB export of the converted data sums up to 2.8 GB of RAW data and will explode on import to the said 28 GB.

1. Create a PostgreSQL Database (we use the name `osmgeocoder` for the DB name and `geocoder` for the DB user in the example)
2. Create the PostGIS, trigram and fuzzy string search extension for the DB:
```sql
CREATE SCHEMA gis;                              -- isolate postgis into its own schema for easier development
ALTER SCHEMA gis OWNER TO geocoder;
CREATE EXTENSION postgis WITH SCHEMA gis;       -- put postgis into gis schema

CREATE SCHEMA str;                              -- isolate string functions into its own schema for easier development
ALTER SCHEMA str OWNER TO geocoder;
CREATE EXTENSION pg_trgm WITH SCHEMA str;       -- trigram search, used for forward geocoding
CREATE EXTENSION fuzzystrmatch WITH SCHEMA str; -- metaphone search, used for text prediction

CREATE SCHEMA crypto;                           -- isolate crypto functions into its own schema for easier development
ALTER SCHEMA crypto OWNER TO geocoder;
CREATE EXTENSION pgcrypto WITH SCHEMA crypto;   -- used to generate uuids

ALTER DATABASE geocoder SET search_path TO public, gis, str, crypto; -- set search path to include the other schemas
```
3. Fetch a copy of [imposm3](https://github.com/omniscale/imposm3)
4. Get a OpenStreetMap data file (for example from [Geofabrik](http://download.geofabrik.de/), start with a small region!)
5. Create a virtualenv and install packages:
```bash
pipenv sync
```
6. See below for importing openaddresses.io data if needed (this is completely optional)
7. Import some OpenStreetMap data into the DB (grab a coffee or two):
```bash
$ bin/prepare_osm.py --db postgresql://geocoder:password@localhost/osmgeocoder --import-data osm.pbf --optimize
```
8. Modify configuration file to match your setup. The example config is in `osmgeocoder/data/config-example.json`.
9. Optionally install and start the postal machine learning address categorizer (see below)
10. Import the geocoding functions into the DB:
```bash
$ bin/finalize_geocoder.py --db postgresql://geocoder:password@localhost/osmgeocoder
```
11. Geocode:
```bash
bin/address2coordinate.py --config config.json --center 48.3849 10.8631 Lauterl
bin/coordinate2address.py --config config.json 48.3849 10.8631
```

For a full example see the ``example_setup.sh`` shell script.

**NOTE:** you can also install this via pip:
- the scripts from the `bin` directory will be copied to your environment.
- An example config file will be placed in your virtualenv in `osmgeocoder/data/config-example.json`
- The PIP installation will not install `flask` and `gunicorn` nor will it try to install `postal`,
  if you want to use those services you need to install those optional dependencies yourself (read on!)


## Optional import of openaddresses.io data

For some countries there are not enough buildings tagged in the OSM data so we can use the [OpenAddresses.io](http://results.openaddresses.io) data to augment the OSM data.

The import is relatively slow as the data is contained in a big bunch of zipped CSV files, we try to use more threads to import the data faster but it could take a while...

### Importing openaddresses.io data

```bash
wget https://s3.amazonaws.com/data.openaddresses.io/openaddr-collected-europe.zip # download openaddress.io data
pipenv run bin/import_openaddress_data.py \ # run an import
    --db postgresql://geocoder:password@host/osmgeocoder \
    --threads 4 \
    --optimize \
    openaddr-collected-europe.zip
```

When you have imported the data it will create some tables in your DB, `license` which contains the licenses of the imported data (the API will return the license attribution string with the data), `oa_city` which is a foreign key target from `oa_street` which in turn is a fk target to `oa_house` which contains the imported data.

If you want to import more than one file, just do so, the tables will not be cleared between import runs, the indices will be dropped and rebuilt after the import though. Skip the `--optimize` flag for the imports and run an optimize only pass last to save some time.

If you want to save even more time import with `--fast`, but be aware this leaves the DB without any indices or foreign key constraints, an optimize pass is required after importing with this flag!

If you want to start over run the command with the `--clean-start` flag... Be careful, this destroys all openaddresses.io data in the tables.


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
pipenv run bin/postal_service.py --config config/config.json
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
pipenv run gunicorn postal_service:app \
    --bind 127.0.0.1:3200 \
    --workers 1 \
    --pid /var/run/postal_service.pid \
    --log-file /var/log/postal_service.log \
    --daemon
```

**Attention**: Every worker takes that 2GB RAM toll!

## Running a HTTP geocoding service

The file `geocoder_service.py` is a simple Flask app to present the geocoder as a HTTP service.

### Installation

```bash
pipenv run pip install gunicorn
pipenv run pip install flask
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
pipenv run gunicorn geocoder_service:app \
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
    - `country`: (optional) ISO Country code, use only if no center coordinate is available as it slows down the geocoder massively.
- Response: Array of objects
    - `address`: Fully written address line, formatted by country standards
    - `lat`: Latitude
    - `lon`: Longitude
    - `license`: License attribution string

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
    - `license`: License attribution string

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
    "user": "osm",
    "password": "password"
  },
  "opencage_data_file": "data/worldwide.yml",
  "postal": {
    "service_url": "http://localhost:3200/",
    "port": 3200
  }
}
```

Keys:

- `db`: Database configuration this will be built into a [Postgres connection string](https://www.postgresql.org/docs/current/static/libpq-connect.html#id-1.7.3.8.3.5)
- `postal` -> `service_url`: (optional) URL where to find the libpostal service, if not supplied searching is reduced to street names only
- `postal` -> `port`: (optional) only used when running the libpostal service directly without explicitly using gunicorn
- `opencage_data_file`: (optional) Data file for the address formatter, defaults to the one included in the package

## API documentation

The complete project contains actually only two classes:

### `Geocoder`.

Publicly accessible method prototypes are:

```python
def __init__(self, db=None, db_handle=None, address_formatter_config=None, postal=None):
    pass

def forward(self, address, country=None, center=None):
    pass

def forward_structured(self, road=None, house_number=None, postcode=None, city=None, country=None, center=None):
    pass

def reverse(self, lat, lon, radius=100, limit=10):
    pass

def reverse_epsg3857(self, x, y, radius=100, limit=10):
    pass

def predict_text(self, input):
    pass
```

#### `__init__`

Initialize a geocoder, this will read all files to be used and set up the DB connection.
- `db`: Dictionary with DB config, when used the geocoder will create a DB-connection on its own
- `db_handle`: Postgres connection, use this if the connection is handled outside the scope of the geocoder (for example when you want to use the geocoder in Django)
- `address_formatter_config`: Path to the `worldwide.yaml` (optional)
- `postal`: Dictionary with postal config (at least `service_url` key)

see __Config File__ above for more info.

#### `forward`

Geocode an address to a lat, lon location.
- `address`: Address to code
- `country`: (optional) Country code to restrict search and format address
- `center`: (optional) Center coordinate to sort results for (will be used to determine country too, so you can skip the `country` flag)

This function is a generator which `yield`s the obtained results.

#### `forward_structured`

Geocode an address to a lat, lon location without using the address classifier, use this if your input is already structured.
- `road`: (optional) Street/Road name
- `house_number`: (optional) House number, this is a string because of things like `1a`
- `postcode`: (optional) Post code, this is a string because not all countries use numbers only and zero prefixes,
- `city`: (optional) City
- `country`: (optional) Country code to restrict search and format address
- `center`: (optional) Center coordinate to sort results for (will be used to determine country too, so you can skip the `country` flag)

Be sure that at least one of `road`, `postcode` or `city` is filled, results are not predictable if none is set.
This function is a generator which `yield`s the obtained results.

#### `reverse`

Geocode a lat, lon location into a readable address:
- `lat`: Latitude to code
- `lon`: Longitute to code
- `radius`: Search radius in meters
- `limit`: (optional) maximum number of results to return

This function is a generator which `yield`s the obtained results.

#### `reverse_epsg3857`

Geocode a x, y location in EPGS 3857 projection (aka Web Mercator) into a readable address:
- `x`: X coordinate
- `y`: Y coordinate
- `radius`: Search radius in meters
- `limit`: (optional) maximum number of results to return

Use this function if you're using Web Mercator in your application internally to avoid constant re-projection between lat, lon and x, y.
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
