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
