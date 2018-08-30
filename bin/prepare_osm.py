#!/usr/bin/env python

import argparse
import psycopg2
from time import time
import io
import os
from pkg_resources import resource_exists, resource_listdir, resource_string

#
# DB-Utility functions
#

def open_db(url):
    conn = psycopg2.connect(url)
    cursor = conn.cursor()
    return cursor

def prepare_db(db):
    # assume we are in a virtualenv first
    sql_files = None
    try:
        if resource_exists('osmgeocoder', 'data/sql'):
            sql_files = list(resource_listdir('osmgeocoder', 'data/sql')) 
            sql_files.sort()
            for f in sql_files:
                print('Executing {}...'.format(f))
                db.execute(resource_string('osmgeocoder', os.path.join('data/sql', f)))
    except ModuleNotFoundError:
        pass
    
    if sql_files is None:
        # if not found, assume we have been started from a source checkout
        my_dir = os.path.dirname(os.path.abspath(__file__))
        sql_path = os.path.abspath(os.path.join(my_dir, '../osmgeocoder/data/sql'))
        sql_files = [f for f in os.listdir(sql_path) if os.path.isfile(os.path.join(sql_path, f))]
    
        sql_files.sort()

        for f in sql_files:
            print('Executing {}...'.format(f))
            with open(os.path.join(sql_path, f), 'r') as fp:
                db.execute(fp.read())

def close_db(db):
    conn = db.connection
    conn.commit()

    db.close()
    conn.close()

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

    return parser.parse_args()


if __name__ == '__main__':
    args = parse_cmdline()
    db = open_db(args.db_url)
    prepare_db(db)
    close_db(db)
