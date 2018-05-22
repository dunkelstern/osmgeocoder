import psycopg2
from psycopg2.extras import RealDictCursor

preferred_table = None


def init_db(db_config):
    global preferred_table

    preferred_table = db_config.pop('table')
    connstring = []
    for key, value in db_config.items():
        connstring.append("{}={}".format(key, value))
    connection = psycopg2.connect(" ".join(connstring))

    return connection


def fetch_address(db, clip_poly, center, limit=10):
    global preferred_table

    query = '''
        SELECT name, street, housenumber, postcode, city, min(distance) as distance FROM (
            SELECT 
              name,
              street,
              housenumber,
              postcode,
              city,
              ST_Distance(geometry, ST_GeomFromText('POINT({x} {y})', 3857)) as distance
            FROM {table}
            WHERE ST_Intersects(
                geometry,
                ST_GeomFromText('{clip_poly}', 3857)
            )
            ORDER BY ST_Distance(geometry, ST_GeomFromText('POINT({x} {y})', 3857))
            LIMIT {limit}
        ) n 
        GROUP BY name, street, housenumber, postcode, city
        ORDER BY min(distance)
    '''.format(
        table=preferred_table,
        clip_poly=clip_poly,
        x=center[0],
        y=center[1],
        limit=limit,
    )

    cursor = db.cursor(cursor_factory=RealDictCursor)
    cursor.execute(query)

    for result in cursor:
        yield result


def fetch_coordinate(db, search_term, center=None, country=None, limit=20):
    global preferred_table

    query = None
    if center is None:
        query = '''
            SELECT 
                *,
                street <-> %s as trgm_dist,
                ST_Centroid(geometry) as location
            FROM {table}
            WHERE
                street %% %s
            ORDER BY trgm_dist DESC
            LIMIT {limit};
        '''.format(
            table=preferred_table,
            limit=limit,
        )
    else:
        query = '''
            SELECT 
                *,
                street <-> %s as trgm_dist,
                ST_Distance(geometry, ST_GeomFromText('POINT({x} {y})', 3857)) as dist,
                ST_Centroid(geometry) as location
            FROM {table}
            WHERE
                street %% %s
            ORDER BY dist ASC, trgm_dist DESC
            LIMIT {limit};
        '''.format(
            table=preferred_table,
            limit=limit,
            x=center[0],
            y=center[1]
        )

    cursor = db.cursor(cursor_factory=RealDictCursor)
    cursor.execute(query, [search_term, search_term])

    for result in cursor:
        yield result
