from shapely.geometry import Point
from shapely.wkb import loads

from pyproj import Proj, transform
from geographiclib.geodesic import Geodesic
from .db import fetch_coordinate

def geocode_forward(db, address, country=None, center=None):
    mercProj = Proj(init='epsg:3857')
    latlonProj = Proj(init='epsg:4326')

    # project center lat/lon to mercator
    merc_coordinate = None
    if center is not None:
        merc_coordinate = transform(latlonProj, mercProj, center[1], center[0])

    results = []
    for coordinate in fetch_coordinate(db, address, center=merc_coordinate):
        p = loads(coordinate['location'], hex=True)

        name = '{street} {housenumber}, {postcode} {city}'.format(**coordinate)
        lon, lat = transform(mercProj, latlonProj, p.x, p.y)
        results.append((
            name, lat, lon
        ))

    return results