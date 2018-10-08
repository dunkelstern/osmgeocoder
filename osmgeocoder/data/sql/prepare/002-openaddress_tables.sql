CREATE TABLE IF NOT EXISTS license (
    id SERIAL PRIMARY KEY,
    website TEXT,
    license TEXT,
    attribution TEXT,
    "source" TEXT
);

CREATE TABLE IF NOT EXISTS city (
    id SERIAL8 PRIMARY KEY,
    city TEXT,
    district TEXT,
    region TEXT,
    postcode TEXT,
    license_id INT
);

CREATE TABLE IF NOT EXISTS street (
    id SERIAL8 PRIMARY KEY,
    street TEXT,
    unit TEXT,
    city_id INT8
);

CREATE TABLE IF NOT EXISTS house (
    id SERIAL8,
    location geometry(POINT, 3857),
    "name" TEXT,
    housenumber TEXT,
    geohash TEXT,
    street_id INT8,
    "source" coordinate_source
) PARTITION BY RANGE (ST_X(location));
