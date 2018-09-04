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
from sys import intern

from tempfile import TemporaryFile

import struct
from binascii import hexlify
from pyproj import Proj, transform

def grouper(n, iterable, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return zip_longest(fillvalue=fillvalue, *args)

class CountingTextIOWrapper(io.TextIOWrapper):
    """Wrapper for the TextIOWrapper to be able to count already consumed bytes"""

    def __init__(self, stream, encoding=None):
        super().__init__(stream, encoding=encoding)
        self.position = 0

    def read(self, *args, **kwargs):
        result = super().read(*args, **kwargs)
        self.position += len(result)
        return result

    def readline(self, *args, **kwargs):
        result = super().readline(*args, **kwargs)
        self.position += len(result)
        return result

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
        DROP TABLE IF EXISTS oa_house;
        DROP TABLE IF EXISTS oa_street;
        DROP TABLE IF EXISTS oa_city;
        DROP TABLE IF EXISTS oa_license;
    ''')

def prepare_db(db):
    print('Creating tables...')
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
    ''')

    print('Dropping indices and constraints for speed improvement...')
    db.execute('''
        ALTER TABLE oa_city DROP CONSTRAINT IF EXISTS oa_city_license_id_fk;
        ALTER TABLE oa_street DROP CONSTRAINT IF EXISTS oa_street_city_id_fk;
        ALTER TABLE oa_house DROP CONSTRAINT IF EXISTS oa_house_street_id_fk;
        DROP INDEX IF EXISTS oa_street_trgm_idx;
        DROP INDEX IF EXISTS oa_city_trgm_idx;
        DROP INDEX IF EXISTS oa_house_trgm_idx;
        DROP INDEX IF EXISTS oa_street_city_id_idx;
        DROP INDEX IF EXISTS oa_house_street_id_idx;
        DROP INDEX IF EXISTS oa_data_location_geohash_idx;
        DROP INDEX IF EXISTS oa_data_location_idx;
    ''')

def finalize_db(db):
    print('Finalizing import and cleaning up...')
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
    print('Adding trigram indices...')
    db.execute('''
        CREATE INDEX IF NOT EXISTS oa_house_trgm_idx ON oa_house USING GIN (housenumber gin_trgm_ops);
        CREATE INDEX IF NOT EXISTS oa_city_trgm_idx ON oa_city USING GIN (city gin_trgm_ops);
        CREATE INDEX IF NOT EXISTS oa_street_trgm_idx ON oa_street USING GIN (street gin_trgm_ops);
        ANALYZE oa_house;
        ANALYZE oa_street;
        ANALYZE oa_city;
    ''')

    print('Adding spatial indices...')
    db.execute('''
        CREATE INDEX oa_house_location_geohash_idx ON oa_house (ST_GeoHash(ST_Transform(location, 4326)));
        CREATE INDEX oa_house_location_idx ON oa_house USING GIST(location);
        ANALYZE oa_house;
    ''')

    print('Clustering houses on geohash...')
    db.execute('''
        CLUSTER oa_house USING oa_house_location_geohash_idx;
        ANALYZE oa_house;
    ''')

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

def import_csv(csv_stream, size, license_id, name, db, line):
    # space optimization, reference these strings instead of copying them
    key_city = intern('city')
    key_streets = intern('streets')
    key_street = intern('street')
    key_houses = intern('houses')

    print("\033[{};0H\033[KPreparing data for {}, 0%...".format(line, name))

    # projection setup, we need WGS84 and WebMercator
    mercProj = Proj(init='epsg:3857')
    latlonProj = Proj(init='epsg:4326')

    # Wrap the byte stream into a TextIOWrapper, we have subclassed it to count
    # the consumed bytes for progress display
    wrapped = CountingTextIOWrapper(csv_stream, encoding='utf8')
    reader = csv.DictReader(wrapped)
    cities = {}
    timeout = time() # status update timeout
    for row in reader:
        # build a street hash
        strt = intern(hashlib.md5(
            (row['STREET'] +
            row['UNIT']).encode('utf8')
        ).hexdigest())

        # build city hash
        cty = intern(hashlib.md5(
            (row['CITY'] +
            row['DISTRICT'] +
            row['REGION'] +
            row['POSTCODE']).encode('utf8')
        ).hexdigest())

        # add city if not already in the list
        if cty not in cities:
            cities[cty] = {
                key_city: (
                    row['CITY'],
                    row['DISTRICT'],
                    row['REGION'],
                    row['POSTCODE']
                ),
                key_streets: {}
            }

        # add street if not already in the list
        if strt not in cities[cty][key_streets]:
            cities[cty][key_streets][strt] = {
                key_street: (
                    row['STREET'],
                    row['UNIT'],
                ),
                key_houses: {}
            }

        # add house to street
        cities[cty][key_streets][strt][key_houses][row['NUMBER']] = (row['LON'], row['LAT'])

        # status update
        if time() - timeout > 1.0:
            print("\033[{};0H\033[KPreparing data for {}, {} %...".format(line, name, round(wrapped.position / size * 100.0,2)))
            timeout = time()

    # force cleaning up to avoid memory bloat
    del reader
    del wrapped
    del csv_stream

    # create a new temporary file for the house data as we use the postgres COPY command with that
    # for speed reasons
    house_file = TemporaryFile(mode='w+')

    # streets and cities have to be inserted the "normal" way because we need their ids
    street_sql = '''
        INSERT INTO oa_street (street, unit, city_id)
        VALUES ($1, $2, $3) RETURNING id
    '''
    city_sql = '''
        INSERT INTO oa_city (city, district, region, postcode, license_id)
        VALUES ($1, $2, $3, $4, $5) RETURNING id
    '''

    # prepare statements
    db.execute('PREPARE street AS {};'.format(street_sql))
    db.execute('PREPARE city AS {};'.format(city_sql))

    # start insertion cycle
    print("\033[{};0H\033[KInserting data for {}...".format(line, name))

    city_count = 0
    row_count = 0
    timeout = time()
    start = timeout
    for key, item in cities.items():
        city_count += 1

        # insert city and fetch the id
        row_count += 1
        db.execute('EXECUTE city (%s, %s, %s, %s, %s);', [*item[key_city], license_id])
        city_id = db.fetchone()[0]

        # insert all streets
        for street in item[key_streets].values():
            row_count += 1

            # we need the id
            db.execute('EXECUTE street (%s, %s, %s);', [*street[key_street], city_id])
            street_id = db.fetchone()[0]

            # houses will not be inserted right away but saved to the temp file
            for nr, location in street[key_houses].items():
                # project into 3857 (mercator) from 4326 (WGS84)
                x, y = transform(latlonProj, mercProj, location[0], location[1])

                # create wkb representation, theoretically we could use shapely
                # but we try to not spam newly created objects here

                # ewkb header + srid
                house_file.write('0101000020110F0000')

                # coordinate
                house_file.write((hexlify(struct.pack('<d', x)) + hexlify(struct.pack('<d', y))).decode('ascii'))

                # house number field
                house_file.write('\t')
                if nr != '':
                    house_file.write(nr.replace('\\', '\\x5c'))
                else:
                    house_file.write(' ')

                # street_id field
                house_file.write('\t')
                house_file.write(str(street_id))

                # next record
                house_file.write('\n')

            # status update
            if time() - timeout > 1.0:
                eta = (len(cities) / city_count * (time() - start)) - (time() - start)
                print("\033[{};0H\033[K - {:40}, {:>6}%, {:>6} rows/second, eta: {:>5} seconds".format(
                    line, name, round((city_count / len(cities) * 100), 2), row_count, int(eta)
                ))
                row_count = 0
                timeout = time()

    del cities

    # now COPY the contents of the temp file into the DB
    print("\033[{};0H\033[K -> Running copy from tempfile ({} MB)...".format(line, round(house_file.tell() / 1024 / 1024, 2)))
    house_file.seek(0)
    db.copy_from(house_file, 'oa_house', columns=('location', 'housenumber', 'street_id'))

    # cleanup
    print("\033[{};0H\033[K -> Inserting for {} took {} seconds.".format(line, name, time() - start))
    db.execute('DEALLOCATE city;')
    db.execute('DEALLOCATE street;')
    house_file.close()


def import_data(filename, threads, db_url):
    # prepare database (drop indices and constraints for speed)
    db = open_db(args.db_url)
    prepare_db(db)

    # insert license data
    z = zipfile.ZipFile(filename)
    files = [f for f in z.namelist() if not f.startswith('summary/') and f.endswith('.csv')]
    files.sort()
    if 'LICENSE.txt' not in z.namelist():
        raise ValueError("Data file does not contain LICENSE.txt which is required")
    licenses = import_licenses(z.read('LICENSE.txt'), db)
    z.close()

    close_db(db)

    # prepare the work queue
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

    # run and wait for all import threads to exit
    if threads == 1:
        for f in import_queue:
            worker(*f)
    else:
        with Pool(threads, maxtasksperchild=1) as p:
            p.starmap(worker, import_queue, 1)

    # clear screen, finalize db (re-create constraints and associated indices)
    print("\033[2J\033[1;0H\033[K")
    db = open_db(args.db_url)
    finalize_db(db)
    close_db(db)


def worker(filename, name, license_id, db_url, status):
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
            status[name] = idx
            break
    if status[name] == -1:
        status[name] = max(seen_lines) + 1

    # open all connections and inputs
    z = zipfile.ZipFile(filename)
    db = open_db(db_url)

    # start the import
    zip_info = z.getinfo(name)
    import_csv(z.open(name, 'r'), zip_info.file_size, license_id, name, db, status[name])

    # clean up afterwards
    close_db(db)
    z.close()

    # free the status line
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
        help='OpenAddresses.io data file (zipped)',
        nargs='?',
        default=None
    )

    return parser.parse_args()


if __name__ == '__main__':
    args = parse_cmdline()
    if args.clean:
        db = open_db(args.db_url)
        clear_db(db)
        close_db(db)
    if args.datafile is not None:
        import_data(args.datafile, args.threads, args.db_url)
    if args.optimize:
        db = open_db(args.db_url)
        optimize_db(db)
        close_db(db)
