#!/usr/bin/env python

try:
    from flask import Flask, jsonify, abort, request
except ImportError:
    print("Error: Please install Flask, `pip install flask`")
    exit(1)

import json
import sys
import os

try:
    from osmgeocoder import Geocoder
except ImportError:
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    from osmgeocoder import Geocoder


app = Flask(__name__)
geocoder = None

@app.before_first_request
def init():
    global geocoder

    # find config file
    config_file = os.environ.get('GEOCODER_CONFIG', None)
    if config_file is None:
        for location in ['~/.osmgeocoderrc', '~/.config/osmgeocoder.json', '/etc/osmgeocoder.json', 'osmgeocoder.json']:
            loc = os.path.expanduser(location)
            if os.path.exists(loc) and os.path.isfile(loc):
                config_file = loc
                break
    if config_file is None:
        raise RuntimeError("No config file found!")

    config = {}
    with open(config_file, "r") as fp:
        config = json.load(fp)

    geocoder = Geocoder(**config)


@app.route('/forward', methods=['POST'])
def forward():
    if not request.is_json:
        abort(400)
    data = request.get_json()
    address = data.get('address', None)
    if address is None:
        abort(400)
    center = data.get('center', None)
    country = data.get('country', None)

    result = []
    results = geocoder.forward(address, center=center, country=country)
    for addr, lat, lon in results:
        result.append({
            "address": ', '.join(addr.split("\n")).strip(),
            "lat": lat,
            "lon": lon
        })

    return jsonify(result)

@app.route('/reverse', methods=['POST'])
def reverse():
    if not request.is_json:
        abort(400)
    data = request.get_json()
    lat = data.get('lat', None)
    lon = data.get('lon', None)
    if lat is None or lon is None:
        abort(400)

    address = next(geocoder.reverse(lat, lon))
    return jsonify({
        "address": ', '.join(address.split("\n")).strip()
    })

@app.route('/predict', methods=['POST'])
def predict():
    if not request.is_json:
        abort(400)
    data = request.get_json()
    query = data.get('query', None)
    if query is None:
        abort(400)

    predictions = list(geocoder.predict_text(query))
    return jsonify({
        "predictions": predictions
    })

# when running this script directly execute gunicorn to serve
if __name__ == "__main__":
    os.execlp(
        "gunicorn",
        "gunicorn",
        "geocoder_service:app",
        "--bind",
        "127.0.0.1:8080"
    )
