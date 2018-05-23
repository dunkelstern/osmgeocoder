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

    cursor = db.cursor(cursor_factory=RealDictCursor)

    # at first throw the address into the neuronal net to get parts
    # FIXME: initialization of postal takes ages, we should re-use the connection in final deployments
    query = '''
    	SELECT *
        FROM jsonb_to_record(
            postal_parse(%s)
        ) AS addr(
            road TEXT,
            house_number TEXT,
            postcode TEXT,
            city TEXT,
            state TEXT,
            country TEXT
        )
    '''

    cursor.execute(query, [search_term])
    parsed_address = cursor.fetchone()

    # sanity check
    ok = False
    for key, value in parsed_address.items():
        if value is not None:
            ok = True
            break

    if not ok:
        # ml model resulted in no parse result, so search for a road then
        parsed_address['road'] = search_term


    #
    # crude query builder following
    #

    # add a where clause for each resolved address field
    q = []
    v = []
    for key, value in parsed_address.items():
        if value is None:
            continue
        q.append('{field} %% %s'.format(field=key))
        v.append(value)

    # create a trigram distance function for sorting
    if 'road' in parsed_address:
        # base it on the road from the address
        trgm_dist = 'road <-> %s as trgm_dist'
        v.insert(0, parsed_address['road'])
    elif 'city' in parsed_address:
        # base it on the city of the address
        trgm_dist = 'city <-> %s as trgm_dist'
        v.insert(0, parsed_address['city'])
    else:
        # no basis for trigram distance
        trgm_dist = '0 as trgm_dist'

    where = " AND ".join(q)

    # run the geocoding query
    query = None
    if center is None:
        # we have no center position, so we can not sort by distance
        query = '''
            SELECT 
                *,
                {trgm_dist},
                ST_Centroid(geometry) as location
            FROM {table}
            WHERE
                {where}
            ORDER BY trgm_dist ASC
            LIMIT {limit};
        '''.format(
            trgm_dist=trgm_dist,
            where=where,
            table=preferred_table,
            limit=limit,
        )
    else:
        # we have a center coordinate, so cluster by the distance to that coordinate

        # Probable optimization: limit search radius to center coordinate
        query = '''
            SELECT 
                *,
                {trgm_dist},
                ST_Distance(ST_Centroid(geometry), ST_GeomFromText('POINT({x} {y})', 3857)) as dist,
                ST_Centroid(geometry) as location
            FROM {table}
            WHERE
                {where}
            ORDER BY trgm_dist ASC, dist ASC 
            LIMIT {limit};
        '''.format(
            trgm_dist=trgm_dist,
            where=where,
            table=preferred_table,
            limit=limit,
            x=center[0],
            y=center[1]
        )

    cursor.execute(query, v)

    for result in cursor:
        yield result
