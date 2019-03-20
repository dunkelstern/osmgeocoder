--
-- Custom address and distance type to avoid repetition
--

DO
$$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'address_and_distance') THEN
		CREATE TYPE public.address_and_distance AS (
			house text,
			road text,
			house_number text,
			postcode text,
			city text,
			location gis.geometry(point, 3857),
			distance float,
			license_id uuid
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
		CREATE TYPE public.coordinate_source AS ENUM ('openaddresses.io', 'openstreetmap');
    END IF;
END
$$;
