#!/usr/bin/env python

import zipfile
import csv
import argparse
import psycopg2
from psycopg2.extras import execute_batch
import io
import os
from pprint import pprint
from multiprocessing import Pool
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
    print('Connecting to {}...'.format(url))
    conn = psycopg2.connect(url)
    cursor = conn.cursor()
    return cursor

def clear_db(db):
    print('Cleaning up')
    db.execute('''
        DROP TABLE IF EXISTS oa_data;
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

        CREATE TABLE IF NOT EXISTS oa_data (
            location geometry(POINT, 3857),
            housenumber TEXT,
            street TEXT,
            unit TEXT,
            city TEXT,
            district TEXT,
            region TEXT,
            postcode TEXT,
            license_id INT
        );

        ALTER TABLE oa_data DROP CONSTRAINT IF EXISTS oa_data_license_id_fk;
        DROP INDEX IF EXISTS oa_data_location_geohash_idx;
        DROP INDEX IF EXISTS oa_data_location_idx;
    ''')

def optimize_db(db):
    print('Adding indexes...')
    db.execute('''
        ALTER TABLE oa_data ADD CONSTRAINT oa_data_license_id_fk FOREIGN KEY license_id REFERENCES oa_license (id) ON DELETE CASCADE INITIALLY DEFERRED;
        CREATE INDEX oa_data_location_geohash_idx ON oa_data (ST_GeoHash(ST_Transform(location, 4326)));
        CREATE INDEX oa_data_location_idx ON oa_data USING GIST(location);
    ''')

    print('Clustering on geohash...')
    db.execute('CLUSTER oa_data ON oa_data_location_geohash_idx;')

def close_db(db):
    print('Committing transaction...')
    conn = db.connection
    conn.commit()

    print('Disconnecting from DB...')
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
            if record['license'] == 'Unknown':
                continue
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

def import_csv(csv_data, license_id, name, db):
    sql = '''
        INSERT INTO oa_data (location, housenumber, street, unit, city, district, region, postcode, license_id)
        VALUES
    '''

    batch_size = 25
    stepsize = 1000

    vals = []
    for i in range(batch_size):
        vals.append('(ST_Transform(ST_SetSRID(ST_MakePoint(${}, ${}), 4326), 3857), ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${})'.format(
            *list(range((i * 10) + 1, (i + 1) * 10 + 1))
        ))

    sql += ',\n'.join(vals)
    db.execute('PREPARE ins AS {};'.format(sql))
    v = ', '.join(['%s, %s, %s, %s, %s, %s, %s, %s, %s, %s' for i in range(batch_size)])

    aggregate = []
    batch = []

    reader = csv.DictReader(io.StringIO(csv_data.decode('UTF-8')))

    row_count = 0
    print(' - preparing batch {} - {} of {}...'.format(row_count, row_count + stepsize * batch_size, name))
    for row in reader:
        row_count += 1

        batch.extend([
            row['LON'],
            row['LAT'],
            row['NUMBER'],
            row['STREET'],
            row['UNIT'],
            row['CITY'],
            row['DISTRICT'],
            row['REGION'],
            row['POSTCODE'],
            license_id
        ])

        if len(batch) == batch_size * 10:
            aggregate.append(batch)
            batch = []

        if len(aggregate) == stepsize:
            print(" + executing batch {} - {} of {}...".format(row_count - len(aggregate) * batch_size + 1, row_count, name))
            execute_batch(db, 'EXECUTE ins (' + v + ');' , aggregate)
            print(' - preparing batch {} - {} of {}...'.format(row_count + 1, row_count + stepsize * batch_size, name))
            aggregate = []

    # execute last incomplete aggregate
    print(' + executing batch {} - {} of {}...'.format(row_count - len(aggregate) * batch_size + 1, row_count, name))
    execute_batch(db, 'EXECUTE ins ( ' + v + ');' , aggregate)
    db.execute('DEALLOCATE ins;')

    # now execute last incomplete batch
    if len(batch) > 0:
        print(' + executing last batch of {}...'.format(name))
        sql = '''
            INSERT INTO oa_data (location, housenumber, street, unit, city, district, region, postcode, license_id)
            VALUES (ST_Transform(ST_SetSRID(ST_MakePoint($1, $2), 4326), 3857), $3, $4, $5, $6, $7, $8, $9, $10)
        '''
        db.execute('PREPARE ins AS {};'.format(sql))

        for item in grouper(10, batch):
            db.execute('EXECUTE ins (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);', item)

        db.execute('DEALLOCATE ins;')




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


    import_queue = []
    for f in files:
        if f not in licenses.keys():
            print('Skipping {}, no license data'.format(f))
            continue
        import_queue.append((filename, f, licenses[f], db_url))

    # wait for all import threads to exit
    if threads == 1:
        for f in import_queue:
            worker(f)
    else:
        with Pool(threads) as p:
            p.map(worker, import_queue)

    db = open_db(args.db_url)
    optimize_db(db)
    close_db(db)


def worker(arg):
    filename, name, license_id, db_url = arg

    print('Importing {}...'.format(name))
    z = zipfile.ZipFile(filename)
    db = open_db(db_url)
    import_csv(z.read(name), license_id, name, db)
    close_db(db)
    z.close()


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
