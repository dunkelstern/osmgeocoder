CREATE TABLE IF NOT EXISTS oa_license (
    id SERIAL PRIMARY KEY,
    website TEXT,
    license TEXT,
    attribution TEXT,
    "source" TEXT
);

CREATE TABLE IF NOT EXISTS oa_city (
    id SERIAL8 PRIMARY KEY,
    city TEXT,
    district TEXT,
    region TEXT,
    postcode TEXT,
    license_id INT
);

CREATE TABLE IF NOT EXISTS oa_street (
    id SERIAL8 PRIMARY KEY,
    street TEXT,
    unit TEXT,
    city_id INT8
);

CREATE TABLE IF NOT EXISTS oa_house (
    id SERIAL8 PRIMARY KEY,
    location geometry(POINT, 3857),
    housenumber TEXT,
    street_id INT8
);
