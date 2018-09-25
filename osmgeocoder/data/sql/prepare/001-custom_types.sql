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
-- Used for attribution messages
--
DO
$$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'coordinate_source') THEN
		CREATE TYPE coordinate_source AS ENUM ('openaddresses.io', 'openstreetmap');
    END IF;
END
$$;

CREATE OR REPLACE FUNCTION get_record_ids(city_in TEXT, postcode_in TEXT, street_in TEXT, housenumber_in TEXT) RETURNS TABLE (
	city_id int8,
	street_id int8,
	house_id int8
) AS
$$
DECLARE
	cty int8;
	strt int8;
	hs int8;
BEGIN
    SELECT
        c.id
    INTO cty
    FROM city c
    WHERE
        c.city = city_in
        AND c.postcode = postcode_in
    LIMIT 1;

	SELECT
	    s.id
	INTO strt
	FROM street s
	WHERE
	    s.city_id = cty
	    AND s.street = street_in
	LIMIT 1;

	SELECT
		h.id
	INTO hs
	FROM house h
	WHERE
		h.street_id = strt
		AND h.housenumber = housenumber_in
	LIMIT 1;

	city_id := cty;
	street_id := strt;
	house_id := hs;
	RETURN NEXT;
END
$$ LANGUAGE 'plpgsql';
