# OSMGeocoder

Python implementation for a OSM Geocoder

## TODO

- Fix imposm importer script, just imports too much
- Do not run geocoder on buildings only

## Quick and dirty how-to

1. Create a PostgreSQL Database with PostGIS activated
2. Fetch a copy of [imposm3](https://github.com/omniscale/imposm3)
3. Get a OpenStreetMap data file (for example from [Geofabrik](http://download.geofabrik.de/), start with a small region!)
4. Import some OpenStreetMap data into the DB:
```bash
$ imposm import -connection postgis://user:password@host:port/database -mapping doc/imposm_mapping.yml -read /path/to/osm.pbf -write -deployproduction
```
5. Create the trigram search extension for the DB:
```sql
CREATE EXTENSION pg_trgm;
```
6. Install the [postal extension](https://github.com/pramsey/pgsql-postal) for PostgreSQL
``` bash
$ git clone https://github.com/pramsey/pgsql-postal.git
$ cd pgsql-postal
$ make && sudo make install
```
```sql
CREATE EXTENSION postal;
```
7. Create a trigram index on the `osm_buildings` table:
```sql
CREATE index road_trgm_idx ON osm_buildings USING GIST (road gist_trgm_ops);
CREATE index city_trgm_idx ON osm_buildings USING GIST (city gist_trgm_ops);
CREATE index postcode_trgm_idx ON osm_buildings USING GIST (postcode gist_trgm_ops);
CREATE index house_number_idx ON osm_buildings using BTREE (house_number);
```
8. Create a virtualenv and install packages:
```bash
mkvirtualenv -p /usr/bin/python3 osmgeocoder
pip install -r requirements.txt
```
8. Geocode:
```bash
python address2coordinate.py --config config/db.json --center 48.3849 10.8631 Lauterl
python coordinate2address.py --config config/db.json 48.3849 10.8631
```
