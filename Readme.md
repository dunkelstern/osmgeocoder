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
6. Create a trigram index on the `osm_buildings` table:
```sql
CREATE index trgm_idx ON osm_buildings USING GIST (street gist_trgm_ops);
```
7. Create a virtualenv and install packages:
```bash
mkvirtualenv -p /usr/bin/python3 osmgeocoder
pip install -r requirements.txt
```
8. Geocode:
```bash
python address2coordinate.py --config config/db.json --country de --center 48.3849 10.8631 Lauter
python coordinate2address.py --config config/db.json 48.3849 10.8631
```
