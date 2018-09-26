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
import geohash

PARTITION_SIZE = 360

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
        DROP VIEW IF EXISTS address_data;
        DROP TABLE IF EXISTS house;
        DROP TABLE IF EXISTS street;
        DROP TABLE IF EXISTS city;
        DROP TABLE IF EXISTS license;
    ''')

def prepare_db(db):
    print('Creating tables...')
    db.execute('''
        DO
        $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'coordinate_source') THEN
                CREATE TYPE coordinate_source AS ENUM ('openaddresses.io', 'openstreetmap');
            END IF;
        END
        $$;

        CREATE TABLE IF NOT EXISTS license (
            id SERIAL PRIMARY KEY,
            website TEXT,
            license TEXT,
            attribution TEXT,
            "source" TEXT
        );

        CREATE TABLE IF NOT EXISTS city (
            id SERIAL8 PRIMARY KEY,
            city TEXT,
            district TEXT,
            region TEXT,
            postcode TEXT,
            license_id INT
        );

        CREATE TABLE IF NOT EXISTS street (
            id SERIAL8 PRIMARY KEY,
            street TEXT,
            unit TEXT,
            city_id INT8
        );

        CREATE TABLE IF NOT EXISTS house (
            id SERIAL8,
            location geometry(POINT, 3857),
            "name" TEXT,
            housenumber TEXT,
            geohash TEXT,
            street_id INT8,
            "source" coordinate_source
        ) PARTITION BY RANGE (ST_X(location));

        --
        -- Re-assembly of openaddresses.io data into one view
        --
        CREATE OR REPLACE VIEW address_data AS (
            SELECT
                h.id,
                h."name",
                s.street,
                h.housenumber,
                c.postcode,
                c.city,
                location,
                h."source"
            FROM house h
            JOIN street s ON h.street_id = s.id
            JOIN city c ON s.city_id = c.id
        );
    ''')

    print('Creating shard tables...')
    min_val = -20026376.39
    max_val = 20026376.39
    val_inc = (max_val - min_val) / PARTITION_SIZE
    print(val_inc)
    for i in range(0, PARTITION_SIZE):
        print(f' - {i}: {min_val + val_inc * i} TO {min_val + val_inc * (i + 1)}')
        db.execute(f'''
            CREATE TABLE IF NOT EXISTS house_{i}
            PARTITION OF house FOR VALUES FROM ({min_val + val_inc * i}) TO ({min_val + val_inc * (i + 1)});

            ALTER TABLE house_{i} DROP CONSTRAINT IF EXISTS house_{i}_street_id_fk;
            DROP INDEX IF EXISTS house_{i}_trgm_idx;
        ''')

    print('Dropping indices and constraints for speed improvement...')
    db.execute('''
        ALTER TABLE city DROP CONSTRAINT IF EXISTS city_license_id_fk;
        ALTER TABLE street DROP CONSTRAINT IF EXISTS street_city_id_fk;
        DROP INDEX IF EXISTS street_trgm_idx;
        DROP INDEX IF EXISTS city_trgm_idx;
        DROP INDEX IF EXISTS street_city_id_idx;
    ''')

def finalize_db(db, optimize=False):
    print('Finalizing import and cleaning up...')
    if optimize is False:
        print('Creating reverse fk indices...')
        for i in range(0, PARTITION_SIZE):
            db.execute(f'''
                CREATE INDEX house_{i}_street_id_idx ON house_{i} USING BTREE(street_id);
                ANALYZE house_{i};
            ''')

        db.execute('''
            CREATE INDEX street_city_id_idx ON street USING BTREE(city_id);
            ANALYZE city;
            ANALYZE street;
        ''')


    print('Creating fk indices...')
    for i in range(0, PARTITION_SIZE):
        db.execute(f'''
            ALTER TABLE house_{i} ADD CONSTRAINT house_{i}_street_id_fk FOREIGN KEY (street_id) REFERENCES street (id) ON DELETE CASCADE ON UPDATE CASCADE INITIALLY DEFERRED;
        ''')
    db.execute('''
        ALTER TABLE city ADD CONSTRAINT city_license_id_fk FOREIGN KEY (license_id) REFERENCES license (id) ON DELETE CASCADE ON UPDATE CASCADE INITIALLY DEFERRED;
        ALTER TABLE street ADD CONSTRAINT street_city_id_fk FOREIGN KEY (city_id) REFERENCES city (id) ON DELETE CASCADE ON UPDATE CASCADE INITIALLY DEFERRED;
    ''')

def cluster(i, url):
    print(f'Running optimize on shard {i}...')
    db = open_db(url)
    db.execute(f'''
        DROP INDEX IF EXISTS house_{i}_street_id_idx;
        DROP INDEX IF EXISTS house_{i}_location_geohash_idx;
        DROP INDEX IF EXISTS house_{i}_trgm_idx;
        DROP INDEX IF EXISTS house_{i}_location_idx;

        CREATE INDEX house_{i}_location_geohash_idx ON house_{i} USING BTREE(geohash);
        CLUSTER house_{i} USING house_{i}_location_geohash_idx;
        CREATE INDEX house_{i}_trgm_idx ON house_{i} USING GIN (housenumber gin_trgm_ops);
        CREATE INDEX house_{i}_location_idx ON house_{i} USING GIST(location);
        ANALYZE house_{i};
    ''')
    close_db(db)

def optimize_db(db, threads, url):
    work_queue = []
    for i in range(0, PARTITION_SIZE):
        work_queue.append((i, url))

    with Pool(threads, maxtasksperchild=1) as p:
        p.starmap(cluster, work_queue, 1)

    print('Adding trigram indices on non-sharded tables...')
    db.execute('''
        CREATE INDEX IF NOT EXISTS city_trgm_idx ON city USING GIN (city gin_trgm_ops);
        CREATE INDEX IF NOT EXISTS street_trgm_idx ON street USING GIN (street gin_trgm_ops);
        ANALYZE street;
        ANALYZE city;
    ''')

    finalize_db(db)

def close_db(db):
    conn = db.connection
    conn.commit()

    db.close()
    conn.close()

#
# Data importer
#

def save_license(record, db):
    sql = 'INSERT INTO license (website, license, attribution, "source") VALUES (%s, %s, %s, %s) RETURNING id;'
    db.execute(sql, (
        record['website'],
        record['license'],
        record['attribution'],
        record['file']
    ))
    return db.fetchone()[0]


def import_licenses(license_data, db):
    licenses = {}
    licenses['osm'] = save_license({
        'file': 'osm',
        'license': 'Open Data Commons Open Database License (ODbL)',
        'attribution': '© OpenStreetMap contributors',
        'website': 'https://www.openstreetmap.org/copyright'
    }, db)

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
            print(f'Saved license for {fname}: {licenses[fname]}')

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

    print(f"\033[{line};0H\033[KPreparing data for {name}, 0%...")

    # projection setup, we need WebMercator
    mercProj = Proj(init='epsg:3857')

    # Wrap the byte stream into a TextIOWrapper, we have subclassed it to count
    # the consumed bytes for progress display
    wrapped = CountingTextIOWrapper(csv_stream, encoding='utf8')
    reader = csv.reader(wrapped)

    # skip header
    reader.__next__()

    cities = {}
    timeout = time() # status update timeout
    for row in reader:
        # build a street hash
        strt = intern(hashlib.md5(
            (row[3] +
            row[4]).encode('utf8')
        ).hexdigest())

        # build city hash
        cty = intern(hashlib.md5(
            (row[5] +
            row[6] +
            row[7] +
            row[8]).encode('utf8')
        ).hexdigest())

        # add city if not already in the list
        if cty not in cities:
            cities[cty] = {
                key_city: (
                    row[5],
                    row[6],
                    row[7],
                    row[8]
                ),
                key_streets: {}
            }

        # add street if not already in the list
        if strt not in cities[cty][key_streets]:
            cities[cty][key_streets][strt] = {
                key_street: (
                    row[3],
                    row[4],
                ),
                key_houses: {}
            }

        # add house to street
        cities[cty][key_streets][strt][key_houses][row[2]] = (row[0], row[1])

        # status update
        if time() - timeout > 1.0:
            percentage = round(wrapped.position / size * 100.0, 2)
            print("\033[{line};0H\033[KPreparing data for {name}, {percentage} %...")
            timeout = time()

    # force cleaning up to avoid memory bloat
    del reader
    del wrapped
    del csv_stream

    # create a new temporary file for the house data as we use the postgres COPY command with that
    # for speed reasons
    house_file = TemporaryFile(mode='w+', buffering=16*1024*1024)

    # streets and cities have to be inserted the "normal" way because we need their ids
    street_sql = '''
        INSERT INTO street (street, unit, city_id)
        VALUES ($1, $2, $3) RETURNING id
    '''
    city_sql = '''
        INSERT INTO city (city, district, region, postcode, license_id)
        VALUES ($1, $2, $3, $4, $5) RETURNING id
    '''

    # prepare statements
    db.execute(f'PREPARE street AS {street_sql};')
    db.execute(f'PREPARE city AS {city_sql};')

    # start insertion cycle
    print(f"\033[{line};0H\033[KInserting data for {name}...")

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
                x, y = mercProj(*location)

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

                # geohash
                house_file.write('\t')
                house_file.write(geohash.encode(float(location[0]), float(location[1])))

                # source
                house_file.write('\t')
                house_file.write('openaddresses.io')

                # street_id field
                house_file.write('\t')
                house_file.write(str(street_id))

                # next record
                house_file.write('\n')

            # status update
            if time() - timeout > 1.0:
                eta = round((len(cities) / city_count * (time() - start)) - (time() - start))
                percentage = round((city_count / len(cities) * 100), 2)
                print(f"\033[{line};0H\033[K - {name:40}, {percentage:>6}%, {row_count:>6} rows/second, eta: {eta:>5} seconds")
                row_count = 0
                timeout = time()

    del cities

    # now COPY the contents of the temp file into the DB
    print(f"\033[{line};0H\033[K -> Running copy from tempfile ({round(house_file.tell() / 1024 / 1024, 2)} MB)...")
    house_file.seek(0)
    db.copy_from(house_file, 'house', columns=('location', 'housenumber', 'geohash', 'source', 'street_id'))

    # cleanup
    print(f"\033[{line};0H\033[K -> Inserting for {name} took {round(time() - start)} seconds.")
    db.execute('DEALLOCATE city;')
    db.execute('DEALLOCATE street;')
    house_file.close()


def import_data(filename, threads, db_url, optimize, fast):
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
    sleep(5)

    # prepare the work queue
    manager = Manager()
    status_object = manager.dict()

    import_queue = []
    for f in files:
        if f not in licenses.keys():
            print(f'Skipping {f}, no license data')
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
    if not fast:
        finalize_db(db, optimize)
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
        help='Re-create indices and cluster the tables on the indices for speed, you can not import any more data after running optimize'
    )
    parser.add_argument(
        '--fast',
        dest='fast',
        default=False,
        action='store_true',
        help='Skip finalizing the Database as this is a multi part import'
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
        import_data(args.datafile, args.threads, args.db_url, args.optimize, args.fast)
    if args.optimize:
        db = open_db(args.db_url)
        optimize_db(db, args.threads, args.db_url)
        close_db(db)
