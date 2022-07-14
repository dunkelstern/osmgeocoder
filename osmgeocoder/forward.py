from typing import Optional, Generator, Tuple, Dict, Any

import psycopg2
from psycopg2.extras import RealDictCursor

from requests import post
from requests.exceptions import ConnectionError


def fetch_coordinate(
    geocoder,
    search_term: str,
    center:Optional[Tuple[float, float]]=None,
    country:Optional[str]=None,
    radius=20000,
    limit=20
) -> Generator[Dict[str, Any], None, None]:
    """
    Fetch probable coordinates from openstreetmap or openaddresses.io data using
    the search term.

    If the postal service is running and configured the search_term will be
    pre-parsed by using that service.

    The results will be sorted by distance to center coordinate, if there is no center
    coordinate the results will be sorted by trigram similarity.

    This is a generator that returns an iterator of dict instances with the following
    keys: house, road, house_number, postcode, city, country, location, trgm_dist, dist

    Not all keys have to be filled at all times.

    :param geocoder: geocoder instance
    :param search_term: user input
    :param center: center coordinate used for distance sorting
    :param country: if set the query will be limited to this country
    :param radius: max search radius around the center coordinate
    :param limit: maximum number of results to return
    """

    if geocoder.postal_service is not None:
        try:
            response = post(geocoder.postal_service['service_url'] + '/split', json={"query": search_term})
            if response.status_code == 200:
                parsed_address = response.json()[0]
            else:
                parsed_address = { 'road': search_term }
        except ConnectionError:
            parsed_address = { 'road': search_term }

    for result in fetch_coordinate_struct(
            geocoder,
            road=parsed_address.get('road', parsed_address.get('house', None)),
            house_number=parsed_address.get('house_number', None),
            postcode=parsed_address.get('postcode', None),
            city=parsed_address.get('city', None),
            country=country,
            center=center,
            radius=radius,
            limit=limit
        ):
        yield result

def fetch_coordinate_struct(
    geocoder,
    road:Optional[str]=None,
    house_number:Optional[str]=None,
    postcode:Optional[str]=None,
    city:Optional[str]=None,
    country:Optional[str]=None,
    center:Optional[Tuple[float, float]]=None,
    radius=20000,
    limit=20
) -> Generator[Dict[str, Any], None, None]:
    """
    Fetch probable coordinates from openstreetmap or openaddresses.io data using
    the structured search terms.

    The results will be sorted by distance to center coordinate, if there is no center
    coordinate the results will be sorted by trigram similarity.

    This is a generator that returns an iterator of dict instances with the following
    keys: house, road, house_number, postcode, city, country, location, trgm_dist, dist

    Not all keys have to be filled at all times.

    :param geocoder: geocoder instance
    :param road: optional, user input
    :param house_number: optional, user input
    :param postcode: optional, user input
    :param city: optional, user input
    :param country: if set the query will be limited to this country
    :param center: center coordinate used for distance sorting
    :param radius: max search radius around the center coordinate
    :param limit: maximum number of results to return
    """

    query = '''
        SELECT * FROM geocode_{typ}(
            %(road)s,
            %(house_number)s,
            %(postcode)s,
            %(city)s,
            %(limit)s,
            ST_Transform(
                ST_SetSRID(
                    ST_MakePoint(%(lon)s, %(lat)s),
                    4326
                ),
                3857
            ),
            %(radius)s,
            %(country)s
        ) LIMIT %(limit)s;
    '''

    cursor = geocoder.db.cursor(cursor_factory=RealDictCursor)

    for typ in ['osm']:  # TODO: Implement for openaddresses.io
        q = query.format(typ=typ)
        cursor.execute(q, {
            'lat': center[0] if center is not None else None,
            'lon': center[1] if center is not None else None,
            'radius': radius,
            'limit': limit,
            'country': country,
            'road': road,
            'house_number': house_number,
            'postcode': postcode,
            'city': city
        })

        if cursor.rowcount > 0:
            break

    for result in cursor:
        yield result