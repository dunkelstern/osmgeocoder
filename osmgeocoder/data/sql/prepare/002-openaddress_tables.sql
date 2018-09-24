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


--
-- Re-assembly of openaddresses.io data into one view
--
-- FIXME: remove admin fallback for city name
CREATE OR REPLACE VIEW address_data AS (
	SELECT
		h.id, s.street, h.housenumber, c.postcode,
		NULLIF(COALESCE(c.city, a.name, NULL), '') AS city, -- fall back to osm city name if none is defined
		location
	FROM house h
	JOIN street s ON h.street_id = s.id
	JOIN city c ON s.city_id = c.id
	LEFT JOIN osm_admin a -- join osm admin table for city name fallback
    	ON (a.admin_level = 6 AND ST_Contains(a.geometry, h.location))
);
