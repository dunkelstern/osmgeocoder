import argparse
import json
from osmgeocoder import *


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

db = init_db(config)


address = geocode_reverse(db, args.lat, args.lon)

print('Resolved {}, {} to "{}"'.format(
    args.lat,
    args.lon,
    address
))
