from typing import Dict, List, Tuple, Any, Generator, Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from shapely.wkb import loads
from pyproj import Proj, transform

from .format import AddressFormatter
from .reverse import fetch_address
from .forward import fetch_coordinate, fetch_coordinate_struct


class Geocoder():

    def __init__(self,
        db:Optional[Dict[str, Any]]=None,
        db_handle=None,
        address_formatter_config:Optional[str]=None,
        postal:Optional[Dict[str, Any]]=None
    ):
        """
        Initialize a new geocoder

        :param db: DB Connection string (mutually exclusive with ``db_handle``)
        :param db_handle: Already opened DB Connection, useful if this connection
                          is handled by a web framework like django
        :param address_formatter_config: Custom configuration for the address formatter,
                                         by default uses the datafile included in the bundle
        :param postal: postal service information, dict with at least ``service_url``
        """
        self.postal_service = postal
        if db is not None:
            self.db = self._init_db(db)
        if db_handle is not None:
            self.db = db_handle
        self.formatter = AddressFormatter(config=address_formatter_config)

    def _init_db(self, db_config:Dict[str, Any]):
        connstring = []
        for key, value in db_config.items():
            connstring.append("{}={}".format(key, value))
        connection = psycopg2.connect(" ".join(connstring))

        return connection

    def forward(
        self,
        address:str,
        country:Optional[str]=None,
        center:Optional[Tuple[float, float]]=None
    ) -> List[Tuple[str, float, float]]:
        """
        Forward geocode address (string -> coordinate tuple) from search string and return formatted address

        :param address: Address to fetch a point for, if you're not running the postal classifier the search
                        will be limited to a street name
        :param country: optional, country name to search in (native language, e.g. "Deutschland" or "France")
        :param center: optional, center coordinate (EPSG 4326/WGS84 (lat, lon) tuple) to sort result by distance
        :returns: List of Tuples of Name, Latitude, Longitude
        """
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

    def forward_structured_dict(
        self,
        road:Optional[str]=None,
        house_number:Optional[str]=None,
        postcode:Optional[str]=None,
        city:Optional[str]=None,
        country:Optional[str]=None,
        center:Optional[Tuple[float, float]]=None
    ) -> List[Dict[str, Any]]:
        """
        Forward geocode address (strings -> coordinate tuple) from structured data and return dictionary

        :param road: Street or road name if known
        :param house_number: House number (string!) if known
        :param postcode: Postcode (string!) if known
        :param city: City name if known
        :param country: optional, country name to search in (native language, e.g. "Deutschland" or "France")
        :param center: optional, center coordinate (EPSG 4326/WGS84 (lat, lon) tuple) to sort result by distance
        :returns: List of Dictionaries with at least 'lat' and 'lon' members
        """
        mercProj = Proj(init='epsg:3857')
        latlonProj = Proj(init='epsg:4326')

        results = []
        for coordinate in fetch_coordinate_struct(
            self, road=road, house_number=house_number,
            postcode=postcode, city=city, country=country,
            center=center):
            
            p = loads(coordinate['location'], hex=True)

            # project location back to lat/lon
            lon, lat = transform(mercProj, latlonProj, p.x, p.y)
            coordinate['lat'] = lat
            coordinate['lon'] = lon

            results.append(coordinate)

        return results

    def forward_structured(
        self,
        road:Optional[str]=None,
        house_number:Optional[str]=None,
        postcode:Optional[str]=None,
        city:Optional[str]=None,
        country:Optional[str]=None,
        center:Optional[Tuple[float, float]]=None
    ) -> List[Tuple[str, float, float]]:
        """
        Forward geocode address (strings -> coordinate tuple) from structured data and return formatted address

        :param road: Street or road name if known
        :param house_number: House number (string!) if known
        :param postcode: Postcode (string!) if known
        :param city: City name if known
        :param country: optional, country name to search in (native language, e.g. "Deutschland" or "France")
        :param center: optional, center coordinate (EPSG 4326/WGS84 (lat, lon) tuple) to sort result by distance
        :returns: List of Tuples of Name, Latitude, Longitude
        """
        data = self.forward_structured_dict(
            road=road,
            house_number=house_number,
            postcode=postcode,
            city=city,
            country=country,
            center=center
        )

        results = []
        for coordinate in data:
            name = self.formatter.format(coordinate)

            results.append((
                name, coordinate['lat'], coordinate['lon']
            ))

        return results

    def reverse_dict(
        self,
        lat:float,
        lon:float,
        radius=100,
        limit=10
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Reverse geocode coordinate to address dictionary

        :param lat: Latitude (EPSG 4326/WGS 84)
        :param lon: Longitude (EPSG 4326/WGS 84)
        :param radius: Search radius
        :param limit: Maximum number of matches to return, defaults to 10
        :returns: generator for addresses formatted to local merit (may contain linebreaks)
        """

        items = fetch_address(self, (lat, lon), radius, projection='epsg:4326', limit=limit)
        for item in items:
            if item is not None:
                yield item

    def reverse(
        self,
        lat:float,
        lon:float,
        radius=100,
        limit=10
    ) -> Generator[str, None, None]:
        """
        Reverse geocode coordinate to address string

        :param lat: Latitude (EPSG 4326/WGS 84)
        :param lon: Longitude (EPSG 4326/WGS 84)
        :param radius: Search radius
        :param limit: Maximum number of matches to return, defaults to 10
        :returns: generator for addresses formatted to local merit (may contain linebreaks)
        """
        items = self.reverse_dict(lat, lon, radius=radius, limit=limit)
        for item in items:
            yield self.formatter.format(item)

    def reverse_epsg3857_dict(
        self,
        x:float,
        y:float,
        radius=100,
        limit=10
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Reverse geocode coordinate to address string
        this one uses the EPSG 3857 aka. Web Mercator projection which is the format
        that is used in the DB already, so by using this function we avoid to re-project
        from and into this projection all the time if we're working with web mercator
        internally. 

        :param x: X (EPSG 3857/Web Mercator)
        :param y: Y (EPSG 3857/Web Mercator)
        :param radius: Search radius
        :param limit: Maximum number of matches to return, defaults to 10
        :returns: generator for addresses formatted to local merit (may contain linebreaks)
        """
        
        items = fetch_address(self, (x, y), radius, projection='epsg:3857', limit=limit)
        for item in items:
            if item is not None:
                yield item

    def reverse_epsg3857(
        self,
        x:float,
        y:float,
        radius=100,
        limit=10
    ) -> Generator[str, None, None]:
        """
        Reverse geocode coordinate to address string
        this one uses the EPSG 3857 aka. Web Mercator projection which is the format
        that is used in the DB already, so by using this function we avoid to re-project
        from and into this projection all the time if we're working with web mercator
        internally. 

        :param x: X (EPSG 3857/Web Mercator)
        :param y: Y (EPSG 3857/Web Mercator)
        :param radius: Search radius
        :param limit: Maximum number of matches to return, defaults to 10
        :returns: generator for addresses formatted to local merit (may contain linebreaks)
        """
        items = self.reverse_epsg3857_dict(x, y, radius=radius, limit=limit)
        for item in items:
            yield self.formatter.format(item)

    def predict_text(self, input:str) -> Generator[str, None, None]:
        """
        Predict word the user is typing currently

        :param input: user input
        :returns: generator for word list, sorted by most common
        """
        query = 'SELECT word FROM predict_text(%s)'

        cursor = self.db.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query, [input])

        for result in cursor:
            yield result['word']
