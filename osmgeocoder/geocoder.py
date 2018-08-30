import os
import psycopg2
from psycopg2.extras import RealDictCursor
from shapely.wkb import loads
from pyproj import Proj, transform

from .format import AddressFormatter
from .reverse import fetch_address
from .forward import fetch_coordinate


class Geocoder():

    def __init__(self, db={}, address_formatter_config=None, postal=None):
        self.postal_service = postal
        self.db = self._init_db(db)
        self.formatter = AddressFormatter(config=address_formatter_config)

    def _init_db(self, db_config):
        connstring = []
        for key, value in db_config.items():
            connstring.append("{}={}".format(key, value))
        connection = psycopg2.connect(" ".join(connstring))

        return connection

    def forward(self, address, country=None, center=None):
        mercProj = Proj(init='epsg:3857')
        latlonProj = Proj(init='epsg:4326')

        results = []
        for coordinate in fetch_coordinate(self, address, country=country, center=center):
            p = loads(coordinate['location'], hex=True)

            name = self.formatter.format(coordinate)

            # project location back to lat/lon
            lon, lat = transform(mercProj, latlonProj, p.x, p.y)
            results.append((
                name, lat, lon
            ))

        return results

    def reverse(self, lat, lon, limit=10):
        for radius in [25, 50, 100]:
            items = fetch_address(self, (lat, lon), radius, limit=limit)
            for item in items:
                if item is not None:
                    yield self.formatter.format(item)

    def predict_text(self, input):
        query = 'SELECT word FROM predict_text(%s)'

        cursor = self.db.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query, [input])

        for result in cursor:
            yield result['word']



 
