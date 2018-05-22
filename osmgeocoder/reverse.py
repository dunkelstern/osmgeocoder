from shapely.geometry import Point
from pyproj import Proj, transform
from geographiclib.geodesic import Geodesic
from .db import fetch_address

def geocode_reverse(db, lat, lon):
    mercProj = Proj(init='epsg:3857')
    latlonProj = Proj(init='epsg:4326')

    # project center lat/lon to mercator
    merc_coordinate = transform(latlonProj, mercProj, lon, lat)

    p = Point(*merc_coordinate)

    for radius in [25, 50, 100]:
        vtx = Geodesic.WGS84.Direct(lat, lon, 45, radius)
        lon2, lat2 = transform(latlonProj, mercProj, vtx['lon2'], vtx['lat2'])
        r = max(abs(lat2 - lat), abs(lon2 - lon))
        poly = p.buffer(r)

        item = next(fetch_address(db, poly, merc_coordinate))
        if item is not None:
            if item['name'] is None or item['name'] == '':
                return '{street} {housenumber}, {postcode} {city}'.format(**item)
            else:
                return '{name}, {street} {housenumber}, {postcode} {city}'.format(**item)
