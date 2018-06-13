import psycopg2
from psycopg2.extras import RealDictCursor

from shapely.wkb import loads

from pyproj import Proj, transform

from requests import post
from requests.exceptions import ConnectionError

from .formatter import AddressFormatter


class Geocoder():

    def __init__(self, config):
        self.config = config
        self.db = self._init_db()
        self.formatter = AddressFormatter(self.config['opencage_data_file'])

    def _init_db(self):

        connstring = []
        for key, value in self.config['db'].items():
            connstring.append("{}={}".format(key, value))
        connection = psycopg2.connect(" ".join(connstring))

        return connection

    def forward(self, address, country=None, center=None):
        mercProj = Proj(init='epsg:3857')
        latlonProj = Proj(init='epsg:4326')

        # project center lat/lon to mercator
        merc_coordinate = None
        if center is not None:
            merc_coordinate = transform(latlonProj, mercProj, center[1], center[0])

        results = []
        for coordinate in self._fetch_coordinate(address, country=country, center=merc_coordinate):
            p = loads(coordinate['location'], hex=True)

            name = self.formatter.format(coordinate)
            lon, lat = transform(mercProj, latlonProj, p.x, p.y)
            results.append((
                name, lat, lon
            ))

        return results

    def reverse(self, lat, lon, limit=10):
        mercProj = Proj(init='epsg:3857')
        latlonProj = Proj(init='epsg:4326')

        # project center lat/lon to mercator
        merc_coordinate = transform(latlonProj, mercProj, lon, lat)

        for radius in [25, 50, 100]:
            items = self._fetch_address(merc_coordinate, radius, limit=limit)
            for item in items:
                if item is not None:
                    yield self.formatter.format(item)

    def predict_text(self, input):
        query = 'SELECT word FROM predict_text(%s)'

        cursor = self.db.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query, [input])

        for result in cursor:
            yield result['word']

    def _fetch_address(self, center, radius, limit=1):
        query = '''
            SELECT house, road, house_number, postcode, city, min(distance) as distance FROM (
                SELECT
                b.name as house,
                b.road,
                b.house_number,
                pc.postcode,
                a.name as city,
                ST_Distance(b.geometry, ST_GeomFromText('POINT({x} {y})', 3857)) as distance
                FROM {buildings_table} b
                LEFT JOIN {postcode_table} pc
                ON ST_Contains(pc.geometry, ST_Centroid(b.geometry))
                LEFT JOIN {admin_table} a
                    ON (a.admin_level = 6 AND ST_Contains(a.geometry, ST_Centroid(b.geometry)))
                WHERE
                    ST_DWithin(
                        b.geometry,
                        ST_GeomFromText('POINT({x} {y})', 3857),
                        {radius}
                    )
                ORDER BY ST_Distance(b.geometry, ST_GeomFromText('POINT({x} {y})', 3857))
                LIMIT {limit}
            ) n
            GROUP BY house, road, house_number, postcode, city
            ORDER BY min(distance)
        '''.format(
            postcode_table=self.config['tables']['postcode'],
            buildings_table=self.config['tables']['buildings'],
            admin_table=self.config['tables']['admin'],
            x=center[0],
            y=center[1],
            radius=radius,
            limit=limit,
        )

        cursor = self.db.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query)

        for result in cursor:
            yield result

    def _fetch_coordinate(self, search_term, center=None, country=None, radius=20000, limit=20):
        cursor = self.db.cursor(cursor_factory=RealDictCursor)

        try:
            response = post(self.config['postal_service_url'] + '/split', json={"query": search_term})
            if response.status_code == 200:
                parsed_address = response.json()[0]
            else:
                parsed_address = { 'road': search_term }
        except ConnectionError:
            parsed_address = { 'road': search_term }

        #
        # crude query builder following
        #

        # add a where clause for each resolved address field
        q = []
        v = []

        # when we have a road search for that
        if 'road' in parsed_address:
            q.append('b.road %% %s')
            v.append(parsed_address['road'])

            # if we have a house additionally search for that
            if 'house' in parsed_address:
                q.append('b.name %% %s')
                v.append(parsed_address['house'])
        elif 'house' in parsed_address:
            # no road, just a house: most likely missclassified
            q.append('b.road %% %s')
            v.append(parsed_address['house'])

        # Add house number to search terms
        if 'house_number' in parsed_address:
            q.append('b.house_number %% %s')
            v.append(parsed_address['house_number'])

        # create a trigram distance function for sorting
        if 'road' in parsed_address or 'house' in parsed_address:
            # base it on the road from the address
            trgm_dist = 'road <-> %s as trgm_dist'
            if 'road' in parsed_address:
                v.insert(0, parsed_address['road'])
            else:
                v.insert(0, parsed_address['house'])
        else:
            # no basis for trigram distance
            trgm_dist = '0 as trgm_dist'

        where = " AND ".join(q)

        # distance sorting
        if center is None:
            distance = ''
            order_by_distance = ''
        else:
            distance = ", ST_Distance(b.geometry, ST_GeomFromText('POINT({x} {y})', 3857)) as dist".format(
                x=center[0],
                y=center[1]
            )
            order_by_distance = 'dist ASC,'
            where = "ST_DWithin(b.geometry, ST_GeomFromText('POINT({x} {y})', 3857), {radius}) AND {where}".format(
                x=center[0],
                y=center[1],
                radius=radius,
                where=where
            )

        if 'postcode' in parsed_address:
            # resolve post code area
            query = '''
                SELECT
                    b.*,
                    pc.postcode,
                    a.name as city,
                    {trgm_dist},
                    ST_Centroid(b.geometry) as location
                    {distance}
                FROM {postcode_table} pc
                JOIN {buildings_table} b
                    ON ST_Intersects(pc.geometry, b.geometry)
                LEFT JOIN {admin_table} a
                    ON (a.admin_level = 6 AND ST_Intersects(a.geometry, b.geometry))
                WHERE
                    pc.postcode = %s
                    AND ({where})
                ORDER BY {order_by_distance} trgm_dist ASC
                LIMIT {limit};
            '''.format(
                trgm_dist=trgm_dist,
                distance=distance,
                postcode_table=self.config['tables']['postcode'],
                buildings_table=self.config['tables']['buildings'],
                admin_table=self.config['tables']['admin'],
                where=where,
                order_by_distance=order_by_distance,
                limit=limit,
            )
            if 'road' in parsed_address:
                v.insert(1, parsed_address['postcode'])
            else:
                v.insert(0, parsed_address['postcode'])
        elif 'city' in parsed_address:
            # run the query by the admin table
            query = '''
                SELECT
                    b.*,
                    pc.postcode
                FROM (
                    SELECT
                        b.*,
                        a.name as city,
                        {trgm_dist},
                        ST_Centroid(b.geometry) as location
                        {distance}
                    FROM {admin_table} a
                    JOIN {buildings_table} b
                        ON ST_Intersects(a.geometry, b.geometry)
                    WHERE
                        a.name %% %s
                        AND a.admin_level = 6
                        AND ({where})
                    ORDER BY {order_by_distance} trgm_dist ASC
                    LIMIT {limit}
                ) b
                LEFT JOIN {postcode_table} pc
                    ON ST_Intersects(pc.geometry, b.geometry)
            '''.format(
                trgm_dist=trgm_dist,
                distance=distance,
                admin_table=self.config['tables']['admin'],
                buildings_table=self.config['tables']['buildings'],
                postcode_table=self.config['tables']['postcode'],
                where=where,
                order_by_distance=order_by_distance,
                limit=limit,
            )
            if 'road' in parsed_address:
                v.insert(1, parsed_address['city'])
            else:
                v.insert(0, parsed_address['city'])
        else:
            # search road name only
            query = '''
                SELECT
                    b.*,
                    pc.postcode,
                    a.name as city
                FROM (
                    SELECT
                        b.*,
                        {trgm_dist},
                        ST_Centroid(b.geometry) as location
                        {distance}
                    FROM
                        {buildings_table} b
                    WHERE
                        {where}
                    ORDER BY
                        {order_by_distance}
                        trgm_dist ASC
                    LIMIT {limit}
                ) b
                LEFT JOIN osm_postal_code pc
                    ON ST_Intersects(pc.geometry, b.geometry)
                LEFT JOIN osm_admin a
                    ON (a.admin_level = 6 AND ST_Intersects(a.geometry, b.geometry))
            '''.format(
                trgm_dist=trgm_dist,
                distance=distance,
                admin_table=self.config['tables']['admin'],
                buildings_table=self.config['tables']['buildings'],
                postcode_table=self.config['tables']['postcode'],
                where=where,
                order_by_distance=order_by_distance,
                limit=limit,
            )

        # run the geocoding query
        try:
            cursor.execute(query, v)
        except psycopg2.ProgrammingError as e:
            print(parsed_address)
            print(query)
            print(v)
            raise e

        for result in cursor:
            yield result
