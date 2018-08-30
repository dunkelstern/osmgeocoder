--
-- Geocode a point to the nearest address
-- This is the Openaddresses.io version for finding an address.
--
DROP FUNCTION IF EXISTS point_to_address_oa(point geometry(point), radius float);
CREATE OR REPLACE FUNCTION point_to_address_oa(point geometry(point), radius float)
RETURNS SETOF address_and_distance AS
$$
	SELECT 
		NULL AS house, -- no house names in openaddresses.io
		s.street as road,
        h.housenumber as house_number,
        c.postcode,
		NULLIF(COALESCE(c.city, a.name), '') AS city, -- fall back to osm city name if none is defined
		location,
		ST_Distance(location, point) as distance
	FROM oa_house h
	JOIN oa_street s ON h.street_id = s.id
	JOIN oa_city c ON s.city_id = c.id
	LEFT JOIN osm_admin a -- join osm admin table for city name fallback
    	ON (a.admin_level = 6 AND ST_Contains(a.geometry, h.location))
	WHERE ST_DWithin(location, point, radius) -- only search within radius
	ORDER BY ST_Distance(location, point) -- order by distance to point
$$ LANGUAGE 'sql';

--
-- Geocode a point to the nearest address
-- This is the OpenStreetMap version which uses defined building geometry for
-- finding an address.
--
DROP FUNCTION IF EXISTS point_to_address_osm(point geometry(point), radius float);
CREATE OR REPLACE FUNCTION point_to_address_osm(point geometry(point), radius float)
RETURNS SETOF address_and_distance AS
$$
	SELECT
		NULLIF(b.name, '') AS house, -- normalize empty string to NULL
		b.road,
		b.house_number,
		pc.postcode,
		a.name AS city,
		ST_Centroid(b.geometry) AS location, -- centroid of building is the location
		ST_Distance(b.geometry, point) AS distance
	FROM osm_buildings b
	LEFT JOIN osm_postal_code pc -- join postal codes which have own geometry
		ON ST_Contains(pc.geometry, ST_Centroid(b.geometry))
	LEFT JOIN osm_admin a -- join admin table to fetch city name, has own geometry
		ON (a.admin_level = 6 AND ST_Contains(a.geometry, ST_Centroid(b.geometry)))
	WHERE ST_DWithin(b.geometry, point, radius) -- only search within radius
	ORDER BY ST_Distance(b.geometry, point) -- order by distance to point
$$ LANGUAGE 'sql';


-- SELECT * FROM point_to_address_oa(ST_Transform(ST_SetSRID(ST_MakePoint(9.738889, 47.550535), 4326), 3857), 250) LIMIT 10;
-- SELECT * FROM point_to_address_osm(ST_Transform(ST_SetSRID(ST_MakePoint(9.738889, 47.550535), 4326), 3857), 250) LIMIT 10;
