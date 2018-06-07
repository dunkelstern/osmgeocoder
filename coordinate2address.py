import argparse
import json
from osmgeocoder import Geocoder


parser = argparse.ArgumentParser(description='OSM Coordinate search')
parser.add_argument(
    '--config',
    type=str,
    nargs=1,
    dest='config',
    required=True,
    help='Config file to use'
)
parser.add_argument(
    'lat',
    type=float,
    help='Latitude to search'
)
parser.add_argument(
    'lon',
    type=float,
    help='Longitude to search'
)

args = parser.parse_args()

config = {}
with open(args.config[0], "r") as fp:
    config = json.load(fp)

geocoder = Geocoder(config)
address = geocoder.reverse(args.lat, args.lon)

print('Resolved {}, {} to "{}"'.format(
    args.lat,
    args.lon,
    ', '.join(address.split("\n")).strip()
))
