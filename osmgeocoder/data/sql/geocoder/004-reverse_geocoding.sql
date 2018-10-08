--
-- Geocode a point to the nearest address
-- This is the Openaddresses.io version for finding an address.
--
DROP FUNCTION IF EXISTS point_to_address(point geometry(point), radius float);
CREATE OR REPLACE FUNCTION point_to_address(point geometry(point), radius float)
RETURNS SETOF address_and_distance AS
$$
	SELECT
		h.name AS house,
		s.street as road,
        h.housenumber as house_number,
        c.postcode,
		c.city,
		location,
		ST_Distance(location, point) as distance,
		c.license_id
	FROM house h
	JOIN street s ON h.street_id = s.id
	JOIN city c ON s.city_id = c.id
	WHERE
		ST_X(location) >= ST_X(point) - radius
		AND ST_X(location) <= ST_X(point) + radius
		AND ST_DWithin(location, point, radius) -- only search within radius
	ORDER BY ST_Distance(location, point) -- order by distance to point
$$ LANGUAGE 'sql';


-- SELECT * FROM point_to_address(ST_Transform(ST_SetSRID(ST_MakePoint(9.738889, 47.550535), 4326), 3857), 250) LIMIT 10;
