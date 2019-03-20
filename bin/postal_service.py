#!/usr/bin/env python

try:
    from flask import Flask, jsonify, abort, request
except ImportError:
    print("Error: Please install Flask, `pip install flask`")
    exit(1)

try:
    from postal.parser import parse_address
    from postal.normalize import normalized_tokens
    from postal.expand import expand_address
    from postal.tokenize import tokenize
except ImportError:
    print("Error: You have to install pypostal, instructions: https://github.com/dunkelstern/osmgeocode/blob/master/Readme.md")
    exit(1)

import json

app = Flask(__name__)

@app.route('/normalize', methods=['POST'])
def normalize():
    if not request.is_json:
        abort(400)
    data = request.get_json()
    query = data['query']
    languages = data.get('languages', None)

    normalized = normalized_tokens(query, languages=languages)
    result = {}
    for value, key in normalized:
        if str(key) not in result:
            result[str(key)] = []
        result[str(key)].append(value)

    return jsonify(result)

@app.route('/split', methods=['POST'])
def split():
    if not request.is_json:
        abort(400)
    data = request.get_json()
    query = data['query']
    language = data.get('language', None)
    country = data.get('country', None)

    # expand address
    if language is not None:
        variants = expand_address(query, languages=[language])
    else:
        variants = expand_address(query)

    result = []
    for variant in variants:
        # then parse
        parts = parse_address(variant, language=language, country=country)

        sub_result = {}
        for value, key in parts:
            sub_result[key] = value

        result.append(sub_result)

    return jsonify(result)

@app.route('/expand', methods=['POST'])
def expand():
    if not request.is_json:
        abort(400)
    data = request.get_json()
    query = data['query']
    languages = data.get('languages', None)

    expanded = expand_address(query, languages=languages)
    tokenized = [tokenize(x) for x in expanded]

    result = []
    for item in tokenized:
        sub_result = []
        for value, _ in item:
            sub_result.append(value)
        result.append(sub_result)

    return jsonify(result)

# when running this script directly execute gunicorn to serve
if __name__ == "__main__":
    import os
    import argparse
    parser = argparse.ArgumentParser(description='Postal address coding service')
    parser.add_argument(
        '--config',
        type=str,
        nargs=1,
        dest='config',
        required=True,
        help='Config file to use'
    )
    args = parser.parse_args()

    config = {}
    with open(args.config[0], "r") as fp:
        config = json.load(fp)

    os.execlp(
        "gunicorn",
        "gunicorn",
        "postal_service:app",
        "--bind",
        "127.0.0.1:{}".format(config['postal']['port'])
    )
