import psycopg2
from psycopg2.extras import RealDictCursor
from pyproj import Proj
from time import time

def prepare_statements(geocoder, num_shards):
    cursor = geocoder.db.cursor()
    for i in range(0, num_shards):
        cursor.execute(f"""
            PREPARE geocode_point_{i} AS
                SELECT
                    h.name AS house,
                    s.street as road,
                    h.housenumber as house_number,
                    c.postcode,
                    c.city,
                    location,
                    ST_Distance(location, $1) as distance,
                    c.license_id
                FROM house_{i} h
                JOIN street s ON h.street_id = s.id
                JOIN city c ON s.city_id = c.id
                WHERE ST_DWithin(location, $1, $2) -- only search within radius
                ORDER BY ST_Distance(location, $1) -- order by distance to point
        """)

def fetch_address(geocoder, center, radius, limit=1):
    """
    Fetch address by searching osm and openaddresses.io data.

    This is a generator and returns an iterator of dicts with the
    following keys: house, road, house_number, postcode, city, distance.

    Not all keys will be filled for all results.

    :param geocoder: the geocoder class instance
    :param center: center coordinate for which to fetch the address
    :param radius: query radius
    :param limit: maximum number of results to return
    """

    min_val = -20026376.39
    max_val = 20026376.39
    val_inc = (max_val - min_val) / 360

    # calculate shard to query
    mercProj = Proj(init='epsg:3857')
    x, y = mercProj(center[1], center[0])
    i = int(x / val_inc) + 180

    query = f'''
        EXECUTE geocode_point_{i}(
            ST_SetSRID(
                ST_MakePoint(%(x)s, %(y)s),
                3857
            ),
            %(radius)s
        );
    '''

    cursor = geocoder.db.cursor(cursor_factory=RealDictCursor)
    cursor.execute(query, { 'x': x, 'y': y, 'radius': radius, 'limit': limit })

    for result in cursor:
        yield result
