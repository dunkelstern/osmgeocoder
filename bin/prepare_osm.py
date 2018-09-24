#!/usr/bin/env python

from time import time
import argparse
import psycopg2
from psycopg2.extras import DictCursor
from time import time
import io
import os
import subprocess
import tempfile
from pkg_resources import resource_exists, resource_listdir, resource_string

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

def convert_osm(db_url):
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

    src = open_db(db_url, cursor_name='converter')
    src.execute("""
        SELECT
            id,
            osm_id,
            name,
            "type",
            COALESCE(
                NULLIF(road, ''::text),
                get_road_name(osm_buildings.*)
            ) AS road,
            house_number,
            geometry,
                CASE
                    WHEN road = ''::text AND get_road_name(osm_buildings.*) IS NOT NULL
                        THEN true
                        ELSE false
                END
                AS extended
        FROM osm_buildings
        WHERE
            "type" <> 'garage'::text
            AND "type" <> 'garages'::text
            AND "type" <> 'shed'::text
            AND "type" <> 'roof'::text
            AND "type" <> 'tank'::text;
    """)

    db.execute('SELECT last_value AS ct FROM osm_buildings_id_seq;')
    rowcount = db.fetchone()['ct']

    rownum = 0
    oldnum = 0
    start = time()
    timeout = time()
    for row in src:
        # try to find the correct entries in the other tables
        # db.execute("""
        # """)
        # print(f'ID: {row["id"]}')

        rownum += 1
        if timeout < time() - 1.0:
            timeout = time()
            eta = round((((time() - start) / rownum) * rowcount) - (time() - start))
            print(f'{rownum}/{rowcount} rows processed: {rownum - oldnum} rows/s, eta: {eta} s')
            oldnum = rownum

    db.close()

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
        convert_osm(args.db_url)
    if args.dump_file:
        dump(args.db_url, args.dump_file)
