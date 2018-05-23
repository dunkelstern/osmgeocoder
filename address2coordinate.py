import argparse
import json
from osmgeocoder import *


parser = argparse.ArgumentParser(description='OSM Address search')
parser.add_argument(
    '--config',
    type=str,
    nargs=1,
    dest='config',
    required=True,
    help='Config file to use'
)
parser.add_argument(
    '--country',
    type=str,
    nargs=1,
    dest='country',
    help='Only search in this country'
)
parser.add_argument(
    '--center',
    type=float,
    nargs=2,
    dest='center',
    help='Center coordinate to filter the results'
)
parser.add_argument(
    'address',
    type=str,
    help='Address to search'
)

args = parser.parse_args()

config = {}
with open(args.config[0], "r") as fp:
    config = json.load(fp)

db = init_db(config)

kwargs = {}
if args.center is not None:
    kwargs['center'] = (args.center[0], args.center[1])
if args.country is not None:
    kwargs['country'] = args.country[0]

results = geocode_forward(db, args.address, **kwargs)

print('Resolved "{}" to'.format(args.address))
for result in results:
    print(" - {} -> {}, {}".format(*result))
