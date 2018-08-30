import psycopg2
from psycopg2.extras import RealDictCursor

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

    query = '''
        SELECT * FROM point_to_address_{typ}(
            ST_Transform(
                ST_SetSRID(
                    ST_MakePoint(%(lon)s, %(lat)s),
                    4326
                ),
                3857
            ),
            %(radius)s
        ) LIMIT %(limit)s;
    '''

    cursor = geocoder.db.cursor(cursor_factory=RealDictCursor)

    for typ in ['osm', 'oa']:
        q = query.format(typ=typ)
        cursor.execute(q, { 'lat': center[0], 'lon': center[1], 'radius': radius, 'limit': limit })

        if cursor.rowcount > 0:
            break

    for result in cursor:
        yield result