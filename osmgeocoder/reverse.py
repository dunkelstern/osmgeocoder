from typing import Tuple, Generator, Dict, Any

import psycopg2
from psycopg2.extras import RealDictCursor
from pyproj import Proj
from time import time

def fetch_address(
    geocoder,
    center:Tuple[float, float],
    radius:float,
    projection='epsg:4326',
    limit=1
) -> Generator[Dict[str, Any], None, None]:
    """
    Fetch address by searching osm and openaddresses.io data.

    This is a generator and returns an iterator of dicts with the
    following keys: house, road, house_number, postcode, city, distance.

    Not all keys will be filled for all results.

    :param geocoder: the geocoder class instance
    :param center: center coordinate for which to fetch the address
    :param radius: query radius
    :param projection: projection type of the coordinate, currently supported: ``epsg:4326`` and ``epsg:3857``
    :param limit: maximum number of results to return
    """

    if projection == 'epsg:4326':
        mercProj = Proj(init='epsg:3857')
        x, y = mercProj(center[1], center[0])
    elif projection == 'epsg:3857':
        x = center[0]
        y = center[1]
    else:
        raise ValueError('Unsupported projection {}'.format(projection))

    query = '''
        SELECT * FROM point_to_address_osm(
            ST_SetSRID(
                ST_MakePoint(%(x)s, %(y)s),
                3857
            ),
            %(radius)s
        ) LIMIT {limit};
    '''.format(limit=int(limit))

    cursor = geocoder.db.cursor(cursor_factory=RealDictCursor)
    cursor.execute(query, { 'x': x, 'y': y, 'radius': radius })

    if cursor.rowcount == 0:
        # try openaddresses.io
        query = '''
        SELECT * FROM point_to_address_oa(
            ST_SetSRID(
                ST_MakePoint(%(x)s, %(y)s),
                3857
            ),
            %(radius)s
        ) LIMIT {limit};
        '''.format(limit=int(limit))
        cursor.execute(query, { 'x': x, 'y': y, 'radius': radius })

    for result in cursor:
        yield result
