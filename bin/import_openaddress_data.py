#!/usr/bin/env python

import zipfile
import csv
import argparse
import psycopg2
from psycopg2.extras import execute_batch
import io
from pprint import pprint

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
            licenses[record['file']] = save_license(record, db)
            print('Saved license for {}: {}'.format(record['file'], licenses[record['file']]))

            record = {
                'file': None,
                'website': None,
                'license': None,
                'attribution': None
            }            
        else:
            record['file'] = line.decode('utf-8').strip()

    return licenses

def import_csv(csv_data, license_id, db):
    sql = '''
        INSERT INTO oa_data (location, housenumber, street, unit, city, district, region, postcode, license_id)
        VALUES (ST_Transform(ST_SetSRID(ST_MakePoint($1, $2), 4326), 3857), $3, $4, $5, $6, $7, $8, $9, $10)
    '''
    db.execute('PREPARE ins AS {};'.format(sql))
    aggregate = []

    row_count = 0
    stepsize = 10000

    reader = csv.DictReader(io.StringIO(csv_data.decode('UTF-8')))
    print(' - preparing batch {} - {}...'.format(row_count, row_count + stepsize))
    for row in reader:
        row_count += 1

        aggregate.append((
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
        ))

        if len(aggregate) == stepsize:
            print(" + executing batch {} - {}...".format(row_count - len(aggregate) + 1, row_count))
            execute_batch(db, 'EXECUTE ins (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);' , aggregate)
            print(' - preparing batch {} - {}...'.format(row_count + 1, row_count + stepsize))
            aggregate = []

    print(' + executing batch {} - {}...'.format(row_count - len(aggregate) + 1, row_count))
    execute_batch(db, 'EXECUTE ins (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);' , aggregate)
    db.execute('DEALLOCATE ins;')

def import_data(filename, db):
    z = zipfile.ZipFile(filename)
    files = [f for f in z.namelist() if not f.startswith('summary/') and f.endswith('.csv')]
    files.sort()
    if 'LICENSE.txt' not in z.namelist():
        raise ValueError("Data file does not contain LICENSE.txt which is required")
    licenses = import_licenses(z.read('LICENSE.txt'), db)
    for f in files:
        name = f.replace('.csv', '')
        if name not in licenses.keys():
            print('Skipping {}, no license data'.format(f))
            continue
        print('Importing {}...'.format(f))
        import_csv(z.read(f), licenses[name], db)
    z.close()

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
        'datafile',
        type=str,
        help='OpenAddresses.io data file (zipped)'
    )

    return parser.parse_args()

def open_db(url):
    print('Connecting to {}...'.format(url))
    conn = psycopg2.connect(url)
    cursor = conn.cursor()

    print('Creating tables')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS oa_license (
            id SERIAL PRIMARY KEY,
            website TEXT,
            license TEXT,
            attribution TEXT,
            "source" TEXT
        );

        CREATE TABLE IF NOT EXISTS oa_data (
            id SERIAL PRIMARY KEY,
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
    return cursor

def close_db(db):
    print('Adding indexes...')
    db.execute('''
        ALTER TABLE oa_data ADD CONSTRAINT oa_data_license_id_fk FOREIGN KEY license_id REFERENCES oa_license (id) ON DELETE CASCADE INITIALLY DEFERRED;
        CREATE INDEX oa_data_location_geohash_idx ON oa_data (ST_GeoHash(ST_Transform(location, 4326)));
        CREATE INDEX oa_data_location_idx ON oa_data USING GIST(location);
    ''')

    print('Clustering on geohash...')
    db.execute('CLUSTER oa_data ON oa_data_location_geohash_idx;')

    print('Committing transaction...')
    conn = db.connection
    conn.commit()

    print('Disconnecting from DB...')
    db.close()
    conn.close()

if __name__ == '__main__':
    args = parse_cmdline()
    db = open_db(args.db_url)
    import_data(args.datafile, db)
    close_db(db)
