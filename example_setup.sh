#!/bin/bash
# run this script as the `postgresql` user to be able to create the DB

# set this to the desired db-password
geocoder_password='***'

# you'll need about 100GB space here:
workdir='/var/tmp/osm'

mkdir -p "$workdir"
cd "$workdir"

# openstreetmap
wget http://download.geofabrik.de/europe-latest.osm.pbf

# openaddresses.io europe
wget https://s3.amazonaws.com/data.openaddresses.io/openaddr-collected-europe.zip
wget https://s3.amazonaws.com/data.openaddresses.io/openaddr-collected-europe-sa.zip

# openaddresses.io switzerland
wget https://s3.amazonaws.com/data.openaddresses.io/runs/212683/ch/aargau.zip
wget https://s3.amazonaws.com/data.openaddresses.io/runs/596898/ch/basel-land.zip
wget https://s3.amazonaws.com/data.openaddresses.io/runs/597245/ch/basel-stadt.zip
wget https://s3.amazonaws.com/data.openaddresses.io/runs/595882/ch/bern.zip
wget https://s3.amazonaws.com/data.openaddresses.io/runs/595777/ch/countrywide.zip
wget https://s3.amazonaws.com/data.openaddresses.io/runs/401113/ch/fribourg.zip
wget https://s3.amazonaws.com/data.openaddresses.io/runs/597073/ch/geneva.zip
wget https://s3.amazonaws.com/data.openaddresses.io/runs/597654/ch/glarus.zip
wget https://s3.amazonaws.com/data.openaddresses.io/runs/597399/ch/grisons.zip
wget https://s3.amazonaws.com/data.openaddresses.io/runs/288191/ch/luzern.zip
wget https://s3.amazonaws.com/data.openaddresses.io/runs/597655/ch/schaffhausen.zip
wget https://s3.amazonaws.com/data.openaddresses.io/runs/597121/ch/solothurn.zip
wget https://s3.amazonaws.com/data.openaddresses.io/runs/597854/ch/uri.zip
wget https://s3.amazonaws.com/data.openaddresses.io/runs/597074/ch/zurich.zip

# imposm binary
wget https://github.com/omniscale/imposm3/releases/download/v0.8.1/imposm-0.8.1-linux-x86-64.tar.gz
tar xvzf imposm-0.8.1-linux-x86-64.tar.gz
export PATH="$PATH:/var/tmp/osm/imposm-0.8.1-linux-x86-64"

# install python stuff
python3 -m venv geocoder-env
. ./geocoder-env/bin/activate

# fetch geocoder scripts
git clone https://github.com/dunkelstern/osmgeocoder.git
cd osmgeocoder
git checkout develop
pip install --upgrade pip
pip install wheel
pip install -r requirements.txt

# create geocoding db
psql <<EOF
CREATE ROLE geocoder WITH LOGIN PASSWORD '$geocoder_password';
CREATE DATABASE osmgeocoder;
ALTER DATABASE osmgeocoder OWNER TO geocoder;
\c osmgeocoder
CREATE SCHEMA gis; -- isolate postgis into its own schema for easier development
ALTER SCHEMA gis OWNER TO geocoder;
CREATE EXTENSION postgis WITH SCHEMA gis; -- put postgis into gis schema
CREATE SCHEMA str; -- isolate string functions into its own schema for easier development
ALTER SCHEMA str OWNER TO geocoder;
CREATE EXTENSION pg_trgm WITH SCHEMA str; -- trigram search, used for forward geocoding
CREATE EXTENSION fuzzystrmatch WITH SCHEMA str; -- metaphone seatch, used for text prediction
CREATE SCHEMA crypto; -- isolate string functions into its own schema for easier development
ALTER SCHEMA crypto OWNER TO geocoder;
CREATE EXTENSION pgcrypto WITH SCHEMA crypto; -- used to generate uuids
ALTER DATABASE geocoder SET search_path TO public, gis, str, crypto; -- set search path to include the other schemas
EOF

# import openaddresses.io data
for item in ../*.zip ; do
    ./bin/import_openaddress_data.py --db postgres://geocoder:$geocoder_password@localhost/osmgeocoder --threads 8 --fast $(realpath $item)
done
./bin/import_openaddress_data.py --db postgres://geocoder:$geocoder_password@localhost/osmgeocoder --threads 8 --optimize

# import osm data
./bin/prepare_osm.py --db postgres://geocoder:$geocoder_password@localhost/osmgeocoder --import-data $(dirname $(pwd))/europe-latest.osm.pbf --optimize --tmpdir $(dirname $(pwd))/imposm_tmp

# run geocoder prepare scripts
./bin/finalize_geocoder.py --db postgres://geocoder:$geocoder_password@localhost/osmgeocoder
