#!/usr/bin/env python

import argparse
import json
import sys
import os

try:
    from osmgeocoder import Geocoder
except ImportError:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
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

geocoder = Geocoder(**config)
address = next(geocoder.reverse(args.lat, args.lon))

print('Resolved {}, {} to "{}"'.format(
    args.lat,
    args.lon,
    ', '.join(address.split("\n")).strip()
))
