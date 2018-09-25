#!/usr/bin/env python

import sys
import io
import os
import math
import random
import argparse
import subprocess
import tempfile

from time import time, sleep
from multiprocessing import Pool, Manager
from pkg_resources import resource_exists, resource_listdir, resource_string

import geohash

import psycopg2
from psycopg2.extras import DictCursor

#
# DB-Utility functions
#

def open_db(url, cursor_name=None):
    conn = psycopg2.connect(url, cursor_factory=DictCursor)
    if cursor_name is None:
        cursor = conn.cursor()
    else:
        cursor = conn.cursor(name=cursor_name)
    return cursor

def load_sql(db, path):
    try:
        # assume we are in a virtualenv first
        if resource_exists('osmgeocoder', path):
            sql_files = list(resource_listdir('osmgeocoder', path))
            sql_files.sort()
            for f in sql_files:
                print('Executing {}...'.format(f))
                db.execute(resource_string('osmgeocoder', os.path.join(path, f)))
    except ModuleNotFoundError:
        # if not found, assume we have been started from a source checkout
        my_dir = os.path.dirname(os.path.abspath(__file__))
        sql_path = os.path.abspath(os.path.join(my_dir, '../osmgeocoder/', path))
        sql_files = [os.path.join(sql_path, f) for f in os.listdir(sql_path) if os.path.isfile(os.path.join(sql_path, f))]
        sql_files.sort()

        for f in sql_files:
            print('Executing {}...'.format(f))
            with open(f, 'r') as fp:
                db.execute(fp.read())


def prepare_db(db):
    load_sql(db, 'data/sql/prepare')

def optimize_db(db):
    load_sql(db, 'data/sql/optimize')

def close_db(db):
    conn = db.connection
    conn.commit()

    if db.name is None:
        db.close()
    conn.close()


def imposm_import(db_url, data_file):
    mapping_file = None
    temp = None
    try:
        # assume we are in a virtualenv first
        if resource_exists('osmgeocoder', 'data/imposm_mapping.yml'):
            data = resource_string('osmgeocoder', 'data/imposm_mapping.yml')
            temp = tempfile.NamedTemporaryFile()
            temp.write(data)
            temp.seek(0)
            mapping_file = temp.name
    except ModuleNotFoundError:
        # if not found, assume we have been started from a source checkout
        my_dir = os.path.dirname(os.path.abspath(__file__))
        mapping_file = os.path.abspath(os.path.join(my_dir, '../osmgeocoder/data/imposm_mapping.yml'))

    subprocess.run(
        'imposm',
        'import',
        '-connection',
        db_url.replace('postgres', 'postgis'),
        '-mapping',
        mapping_file,
        '-read',
        data_file,
        '-write',
        '-optimize',
        '-deployproduction'
    )

    if temp is not None:
        temp.close()

def convert_osm(db_url, threads):
    print('Converting OSM data...')

    db = open_db(db_url)
    db.execute("""
        CREATE OR REPLACE FUNCTION get_road_name(building osm_buildings) RETURNS text AS
        $$
            SELECT
                road
            FROM osm_roads r
            WHERE
                ST_DWithin(building.geometry, r.geometry, 25)
                AND road IS NOT NULL
                AND road <> ''
            ORDER BY ST_Distance(building.geometry, r.geometry) ASC
            LIMIT 1;
        $$ LANGUAGE sql IMMUTABLE;
    """)


    print('Prefetching country -> postcode mapping...')
    src = open_db(db_url, cursor_name='converter')
    src.execute("""
        SELECT
            array_agg(pc.id) as id,
            a.name
        FROM osm_postal_code pc
        JOIN osm_admin a ON (a.admin_level = 2 AND ST_Contains(a.geometry, ST_Centroid(pc.geometry)))
        WHERE NULLIF(a.name, '') IS NOT NULL
        GROUP BY a.id
    """)

    country_postcode_map = {}
    for row in src:
        country_postcode_map[row['name']] = list(row['id'])
        print(f'- Country {row["name"]} has {len(row["id"])} postcode areas')

    postcode_map = {}
    for country, ids in country_postcode_map.items():
        print(f'Prefetching city -> postcode mapping for {country}...')

        src = open_db(db_url, cursor_name='converter')
        src.execute("""
            SELECT
                pc.id,
                pc.postcode,
                a.name as city
            FROM osm_postal_code pc
            LEFT JOIN osm_admin a -- join admin table to fetch city name, has own geometry
                ON (a.admin_level = 8 AND ST_Contains(a.geometry, ST_Centroid(pc.geometry)))
            WHERE
                NULLIF(pc.postcode, '') IS NOT NULL
                AND a.id IS NOT NULL
                AND pc.id = ANY(%s);
        """, [ids])

        for row in src:
            postcode_map[row['id']] = (row['postcode'], sys.intern(row['city']), sys.intern(country))
        close_db(src)

    del country_postcode_map

    # prepare the work queue
    manager = Manager()
    status_object = manager.dict()

    # approximate row count
    db.execute('SELECT last_value AS ct FROM osm_buildings_id_seq;')
    rowcount = db.fetchone()['ct']
    part_size = int(math.ceil(rowcount / (threads * 10000)))

    import_queue = []
    for id_start in range(0, rowcount - part_size, part_size):
        status_object[str(id_start)] = -1
        import_queue.append((id_start, id_start + part_size, postcode_map, db_url, status_object))

    print("\033[2J")
    status_object['__dummy__'] = 0

    # run and wait for all import threads to exit
    if threads == 1:
        for f in import_queue:
            worker(*f)
    else:
        with Pool(threads, maxtasksperchild=1) as p:
            p.starmap(worker, import_queue, 1)

    print("\033[2J\033[1;0H\033[K")
    close_db(db)

def worker(id_start, id_end, postcode_map, db_url, status):
    # wait a random time to make the status line selection robust
    sleep(random.random() * 3.0 + 0.5)

    # select which line we want to use to send our status output to
    seen_lines = []
    for value in status.values():
        if value >= 0 and value not in seen_lines:
            seen_lines.append(value)
    seen_lines.sort()
    for idx, l in enumerate(seen_lines):
        if idx != l:
            status[str(id_start)] = idx
            break
    if status[str(id_start)] == -1:
        status[str(id_start)] = max(seen_lines) + 1

    _convert(id_start, id_end, postcode_map, db_url, status[str(id_start)])

    status[str(id_start)] = -1

def _convert(id_start, id_end, postcode_map, db_url, line):
    db = open_db(db_url)
    src = open_db(db_url, cursor_name='converter')
    src.itersize = 100
    src.execute("""
        SELECT
            b.id,
            b.osm_id,
            b.name,
            b."type",
            pc.id as pc_id,
            COALESCE(
                NULLIF(b.road, ''::text),
                get_road_name(b.*)
            ) AS road,
            b.house_number,
            ST_Centroid(b.geometry) as location,
            ST_X(ST_Transform(ST_Centroid(b.geometry), 4326)) as lat,
            ST_Y(ST_Transform(ST_Centroid(b.geometry), 4326)) as lon
        FROM osm_buildings b
        LEFT JOIN osm_postal_code pc -- join postal codes which have own geometry
            ON ST_Contains(pc.geometry, ST_Centroid(b.geometry))
        WHERE
            b.id >= %s
            AND b.id < %s
            AND pc.id IS NOT NULL
            AND b."type" <> 'garage'::text
            AND b."type" <> 'garages'::text
            AND b."type" <> 'shed'::text
            AND b."type" <> 'roof'::text
            AND b."type" <> 'tank'::text;
    """, (id_start, id_end))

    rownum = 0
    oldnum = 0
    skipped = 0
    start = time()
    timeout = time()
    for row in src:
        rownum += 1
        if timeout < time() - 1.0:
            rows_per_sec = (rownum - oldnum) / (time() - timeout)
            timeout = time()
            print(f'\033[{line};0H\033[K{id_start}: {rownum} rows processed, {skipped} skipped, {round(rows_per_sec)} rows/s, time elapsed: {round(time() - start)} s')
            oldnum = rownum

        if row['pc_id'] is None or row['road'] is None:
            skipped += 1
            continue

        try:
            postcode, city, country = postcode_map[row['pc_id']]
            db.execute("SELECT * FROM get_record_ids(%s::text, %s::text, %s::text, %s::text);", (city, postcode, row['road'], row['house_number']))
            result = db.fetchone()
        except KeyError:
            skipped += 1
            continue

        city_id = result['city_id']
        if city_id is None:
            # create city
            db.execute('INSERT INTO city (city, postcode, license_id) VALUES (%s, %s, 1) RETURNING id;', (city, postcode))
            city_id = db.fetchone()['id']

        street_id = result['street_id']
        if street_id is None:
            # create street
            db.execute('INSERT INTO street (street, city_id) VALUES (%s, %s) RETURNING id;', (row['road'], city_id))
            street_id = db.fetchone()['id']

        house_id = result['house_id']
        if house_id is None:
            # create house
            geo = geohash.encode(row['lat'], row['lon'])
            db.execute(
                'INSERT INTO house (location, name, housenumber, geohash, street_id, source) VALUES (%s, %s, %s, %s, %s, \'openstreetmap\') RETURNING id;',
                (row['location'], row['name'], row['house_number'], geo, street_id)
            )
            house_id = db.fetchone()['id']

        # FIXME: update house name

    close_db(db)

def dump(db_url, filename):
    print(f'Dumping database into {filename}...')
    print('Not implemented')

#
# Cmdline interface
#

def parse_cmdline():
    parser = argparse.ArgumentParser(description='OpenStreetMap Geocoder preparation script')
    parser.add_argument(
        '--db',
        type=str,
        dest='db_url',
        required=True,
        help='Postgis DB URL'
    )
    parser.add_argument(
        '--import-data',
        type=str,
        dest='data_file',
        help='OpenStreetMap data file to import'
    )
    parser.add_argument(
        '--optimize',
        dest='optimize',
        action='store_true',
        default=False,
        help='Optimize DB Tables and create indices'
    )
    parser.add_argument(
        '--convert',
        dest='convert',
        action='store_true',
        default=False,
        help='Convert OpenStreetMap data into a compact form, this needs openaddress data imported first to be efficient'
    )
    parser.add_argument(
        '--dump',
        type=str,
        dest='dump_file',
        help='Dump the converted data into a pg_dump file to be imported on another server'
    )
    parser.add_argument(
        '--threads',
        type=int,
        dest='threads',
        default=1,
        help='Number of convert threads'
    )

    return parser.parse_args()


if __name__ == '__main__':
    args = parse_cmdline()
    db = open_db(args.db_url)
    prepare_db(db)
    if args.data_file is not None:
        imposm_import(args.db_url, args.data_file)
    if args.optimize:
        optimize_db(db)
    close_db(db)
    if args.convert:
        convert_osm(args.db_url, args.threads)
    if args.dump_file:
        dump(args.db_url, args.dump_file)
