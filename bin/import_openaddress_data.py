#!/usr/bin/env python

import zipfile
import csv
import argparse
import psycopg2
import hashlib
import random
from time import time, sleep
from psycopg2.extras import execute_batch
import io
import os
from pprint import pprint
from multiprocessing import Pool, Manager
from itertools import zip_longest


def grouper(n, iterable, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return zip_longest(fillvalue=fillvalue, *args)
#
# DB-Utility functions
#

def open_db(url):
    conn = psycopg2.connect(url)
    cursor = conn.cursor()
    return cursor

def clear_db(db):
    print('Cleaning up')
    db.execute('''
        DROP TABLE IF EXISTS oa_city;
        DROP TABLE IF EXISTS oa_street;
        DROP TABLE IF EXISTS oa_house;
        DROP TABLE IF EXISTS oa_license;
    ''')

def prepare_db(db):
    print('Creating tables')
    db.execute('''
        CREATE TABLE IF NOT EXISTS oa_license (
            id SERIAL PRIMARY KEY,
            website TEXT,
            license TEXT,
            attribution TEXT,
            "source" TEXT
        );

        CREATE TABLE IF NOT EXISTS oa_city (
            id SERIAL8 PRIMARY KEY,
            city TEXT,
            district TEXT,
            region TEXT,
            postcode TEXT,
            license_id INT
        );

        CREATE TABLE IF NOT EXISTS oa_street (
            id SERIAL8 PRIMARY KEY,
            street TEXT,
            unit TEXT,
            city_id INT8
        );

        CREATE TABLE IF NOT EXISTS oa_house (
            id SERIAL8 PRIMARY KEY,
            location geometry(POINT, 3857),
            housenumber TEXT,
            street_id INT8
        );

        ALTER TABLE oa_city DROP CONSTRAINT IF EXISTS oa_city_license_id_fk;
        ALTER TABLE oa_street DROP CONSTRAINT IF EXISTS oa_street_city_id_fk;
        ALTER TABLE oa_house DROP CONSTRAINT IF EXISTS oa_house_street_id_fk;
        DROP INDEX IF EXISTS oa_street_city_id_idx;
        DROP INDEX IF EXISTS oa_house_street_id_idx;
        DROP INDEX IF EXISTS oa_data_location_geohash_idx;
        DROP INDEX IF EXISTS oa_data_location_idx;
    ''')

def finalize_db(db):
    db.execute('''
        CREATE INDEX oa_street_city_id_idx ON oa_street USING BTREE(city_id);
        CREATE INDEX oa_house_street_id_idx ON oa_house USING BTREE(street_id);
        ANALYZE oa_city;
        ANALYZE oa_street;
        ANALYZE oa_house;
        ALTER TABLE oa_city ADD CONSTRAINT oa_city_license_id_fk FOREIGN KEY (license_id) REFERENCES oa_license (id) ON DELETE CASCADE ON UPDATE CASCADE INITIALLY DEFERRED;
        ALTER TABLE oa_street ADD CONSTRAINT oa_street_city_id_fk FOREIGN KEY (city_id) REFERENCES oa_city (id) ON DELETE CASCADE ON UPDATE CASCADE INITIALLY DEFERRED;
        ALTER TABLE oa_house ADD CONSTRAINT oa_house_street_id_fk FOREIGN KEY (street_id) REFERENCES oa_street (id) ON DELETE CASCADE ON UPDATE CASCADE INITIALLY DEFERRED;
    ''')

def optimize_db(db):
    print('Adding indexes...')
    db.execute('''
        CREATE INDEX oa_house_location_geohash_idx ON oa_house (ST_GeoHash(ST_Transform(location, 4326)));
        CREATE INDEX oa_house_location_idx ON oa_house USING GIST(location);
        ANALYZE oa_house;
    ''')

    print('Clustering on geohash...')
    db.execute('CLUSTER oa_house USING oa_house_location_geohash_idx;')

def close_db(db):
    conn = db.connection
    conn.commit()

    db.close()
    conn.close()

#
# Data importer
#

def save_license(record, db):
    sql = 'INSERT INTO oa_license (website, license, attribution, "source") VALUES (%s, %s, %s, %s) RETURNING id;'
    db.execute(sql, (
        record['website'],
        record['license'],
        record['attribution'],
        record['file']
    ))
    return db.fetchone()[0]


def import_licenses(license_data, db):
    licenses = {}

    lines = license_data.split(b"\n")[2:] # skip header

    record = {
        'file': None,
        'website': None,
        'license': None,
        'attribution': None
    }
    for line in lines:
        if line.startswith(b'Website:'):
            record['website'] = line[8:].decode('utf-8').strip()
        elif line.startswith(b'License:'):
            record['license'] = line[8:].decode('utf-8').strip()
        elif line.startswith(b'Required attribution:'):
            a = line[21:].decode('utf-8').strip()
            if a != 'Yes':
                record['attribution'] = a
        elif len(line) == 0:
            # if record['license'] == 'Unknown':
            #     continue
            fname = record['file'] + '.csv'
            licenses[fname] = save_license(record, db)
            print('Saved license for {}: {}'.format(fname, licenses[fname]))

            record = {
                'file': None,
                'website': None,
                'license': None,
                'attribution': None
            }
        else:
            record['file'] = line.decode('utf-8').strip()

    return licenses

def import_csv(csv_data, license_id, name, db, line):
    print("\033[{};0H\033[KPreparing data for {}...".format(line, name))

    reader = csv.DictReader(io.StringIO(csv_data.decode('UTF-8')))
    cities = {}
    for row in reader:
        # build a street hash
        strt = hashlib.md5(
            (row['STREET'] +
            row['UNIT']).encode('utf8')
        ).hexdigest()

        cty = hashlib.md5(
            (row['CITY'] +
            row['DISTRICT'] +
            row['REGION'] +
            row['POSTCODE']).encode('utf8')
        ).hexdigest()

        if cty not in cities:
            cities[cty] = {
                'city': (
                    row['CITY'],
                    row['DISTRICT'],
                    row['REGION'],
                    row['POSTCODE']
                ),
                'streets': {}
            }

        if strt not in cities[cty]['streets']:
            cities[cty]['streets'][strt] = {
                'street': (
                    row['STREET'],
                    row['UNIT'],
                ),
                'houses': {}
            }

        cities[cty]['streets'][strt]['houses'][row['NUMBER']] = (row['LON'], row['LAT'])

    del reader
    del csv_data

    house_sql = '''
        INSERT INTO oa_house (location, housenumber, street_id)
        VALUES (ST_Transform(ST_SetSRID(ST_MakePoint($1, $2), 4326), 3857), $3, $4)
    '''
    street_sql = '''
        INSERT INTO oa_street (street, unit, city_id)
        VALUES ($1, $2, $3) RETURNING id
    '''
    city_sql = '''
        INSERT INTO oa_city (city, district, region, postcode, license_id)
        VALUES ($1, $2, $3, $4, $5) RETURNING id
    '''

    db.execute('PREPARE house AS {};'.format(house_sql))
    db.execute('PREPARE street AS {};'.format(street_sql))
    db.execute('PREPARE city AS {};'.format(city_sql))

    print("\033[{};0H\033[KInserting data for {}...".format(line, name))

    city_count = 0
    row_count = 0
    timeout = time()
    start = timeout
    for key, item in cities.items():
        city_count += 1

        row_count += 1
        db.execute('EXECUTE city (%s, %s, %s, %s, %s);', [*item['city'], license_id])
        city_id = db.fetchone()[0]

        for street in item['streets'].values():
            row_count += 1
            db.execute('EXECUTE street (%s, %s, %s);', [*street['street'], city_id])
            street_id = db.fetchone()[0]

            aggregate = []
            for nr, location in street['houses'].items():
                row_count += 1
                aggregate.append((location[0], location[1], nr, street_id))

            execute_batch(db, 'EXECUTE house (%s, %s, %s, %s);' , aggregate)

            if time() - timeout > 1.0:
                eta = (len(cities) / city_count * (time() - start)) - (time() - start)
                print("\033[{};0H\033[K - {:40}, {:>6}%, {:>6} rows/second, eta: {:>5} seconds".format(
                    line, name, round((city_count / len(cities) * 100), 2), row_count, int(eta)
                ))
                row_count = 0
                timeout = time()

    print("\033[{};0H\033[K -> Inserting for {} took {} seconds.".format(line, name, time() - start))

    db.execute('DEALLOCATE city;')
    db.execute('DEALLOCATE street;')
    db.execute('DEALLOCATE house;')


def import_data(filename, threads, db_url):
    db = open_db(args.db_url)
    prepare_db(db)

    z = zipfile.ZipFile(filename)
    files = [f for f in z.namelist() if not f.startswith('summary/') and f.endswith('.csv')]
    files.sort()
    if 'LICENSE.txt' not in z.namelist():
        raise ValueError("Data file does not contain LICENSE.txt which is required")
    licenses = import_licenses(z.read('LICENSE.txt'), db)
    z.close()

    close_db(db)

    manager = Manager()
    status_object = manager.dict()

    import_queue = []
    for f in files:
        if f not in licenses.keys():
            print('Skipping {}, no license data'.format(f))
            continue
        status_object[f] = -1
        import_queue.append((filename, f, licenses[f], db_url, status_object))

    print("\033[2J")
    status_object['__dummy__'] = 0

    # wait for all import threads to exit
    if threads == 1:
        for f in import_queue:
            worker(*f)
    else:
        with Pool(threads, maxtasksperchild=1) as p:
            p.starmap(worker, import_queue, 1)

    print("\033[2J")
    db = open_db(args.db_url)
    finalize_db(db)
    close_db(db)


def worker(filename, name, license_id, db_url, status):
    sleep(random.random() * 3.0 + 0.5)
    seen_lines = []
    for value in status.values():
        if value >= 0 and value not in seen_lines:
            seen_lines.append(value)
    seen_lines.sort()
    for idx, l in enumerate(seen_lines):
        if idx != l:
            status[name] = idx
            break
    if status[name] == -1:
        status[name] = max(seen_lines) + 1

    z = zipfile.ZipFile(filename)
    db = open_db(db_url)
    import_csv(z.read(name), license_id, name, db, status[name])
    close_db(db)
    z.close()

    status[name] = -1


#
# Cmdline interface
#

def parse_cmdline():
    parser = argparse.ArgumentParser(description='OpenAddresses.io data importer')
    parser.add_argument(
        '--db',
        type=str,
        dest='db_url',
        required=True,
        help='Postgis DB URL'
    )
    parser.add_argument(
        '--threads',
        type=int,
        dest='threads',
        default=1,
        help='Number of import threads'
    )
    parser.add_argument(
        '--clean-start',
        dest='clean',
        default=False,
        action='store_true',
        help='Drop tables before importing'
    )
    parser.add_argument(
        '--optimize',
        dest='optimize',
        default=False,
        action='store_true',
        help='Re-create indices and cluster the tables on the indices for speed'
    )
    parser.add_argument(
        'datafile',
        type=str,
        help='OpenAddresses.io data file (zipped)'
    )

    return parser.parse_args()


if __name__ == '__main__':
    args = parse_cmdline()
    if args.clean:
        db = open_db(args.db_url)
        clear_db(db)
        close_db(db)
    import_data(args.datafile, args.threads, args.db_url)
    if args.optimize:
        db = open_db(args.db_url)
