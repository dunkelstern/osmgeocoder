import os
import psycopg2
from psycopg2.extras import RealDictCursor
from shapely.wkb import loads
from pyproj import Proj, transform

from .format import AddressFormatter
from .reverse import fetch_address
from .forward import fetch_coordinate


class Geocoder():

    def __init__(self, db=None, db_handle=None, address_formatter_config=None, postal=None):
        """
        Initialize a new geocoder

        :param db: DB Connection string (mutually exclusive with ``db_handle``)
        :param db_handle: Already opened DB Connection, useful if this connection
                          is handled by a web framework like django
        :param address_formatter_config: Custom configuration for the address formatter,
                                         by default uses the datafile included in the bundle
        :param postal:
        """
        self.postal_service = postal
        if db is not None:
            self.db = self._init_db(db)
        if db_handle is not None:
            self.db = db_handle
        self.formatter = AddressFormatter(config=address_formatter_config)

    def _init_db(self, db_config):
        connstring = []
        for key, value in db_config.items():
            connstring.append(f"{key}={value}")
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
