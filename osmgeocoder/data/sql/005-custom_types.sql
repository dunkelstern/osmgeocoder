--
-- Custom address and distance type to avoid repetition
--

DO
$$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'address_and_distance') THEN
		CREATE TYPE address_and_distance AS (
			house text,
			road text,
			house_number text,
			postcode text,
			city text,
			location geometry(point),
			distance float
		);
    END IF;
END
$$;


--
-- Re-assembly of openaddresses.io data into one view
--
CREATE OR REPLACE VIEW oa_data AS (
	SELECT 
		h.id, s.street, h.housenumber, c.postcode,
		NULLIF(COALESCE(c.city, a.name, NULL), '') AS city, -- fall back to osm city name if none is defined
		location 
	FROM oa_house h
	JOIN oa_street s ON h.street_id = s.id
	JOIN oa_city c ON s.city_id = c.id
	LEFT JOIN osm_admin a -- join osm admin table for city name fallback
    	ON (a.admin_level = 6 AND ST_Contains(a.geometry, h.location))
);
