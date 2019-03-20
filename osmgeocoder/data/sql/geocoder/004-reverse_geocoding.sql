--
-- Geocode a point to the nearest address
-- This is the Openaddresses.io version for finding an address.
--
DROP FUNCTION IF EXISTS public.point_to_address_oa(point geometry(point), radius float);
CREATE OR REPLACE FUNCTION public.point_to_address_oa(point geometry(point), radius float)
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
	FROM public.oa_house h
	JOIN public.oa_street s ON h.street_id = s.id
	JOIN public.oa_city c ON s.city_id = c.id
	WHERE
		ST_X(location) >= ST_X(point) - radius
		AND ST_X(location) <= ST_X(point) + radius
		AND ST_DWithin(location, point, radius) -- only search within radius
	ORDER BY ST_Distance(location, point) -- order by distance to point
$$ LANGUAGE 'sql';

--
-- Geocode a point to the nearest address
-- This is the OpenStreetMap version for finding an address.
--
DROP FUNCTION IF EXISTS point_to_address_osm(point geometry(point), radius float);
CREATE OR REPLACE FUNCTION point_to_address_osm(point geometry(point), radius float)
RETURNS SETOF address_and_distance AS
$$
	SELECT
		'' AS house,
		s.name as road,
        h.house_number,
        c.postcode,
		c.name as city,
		h.geometry as location,
		ST_Distance(h.geometry, point) as distance,
		(SELECT id FROM oa_license WHERE source = 'osm' LIMIT 1) as license_id
	FROM public.osm_struct_house h
	JOIN public.osm_struct_street s ON h.street_id = s.id
	JOIN public.osm_struct_city c ON s.city_id = c.id
	WHERE
		ST_X(location) >= ST_X(point) - radius
		AND ST_X(location) <= ST_X(point) + radius
		AND ST_DWithin(location, point, radius) -- only search within radius
	ORDER BY ST_Distance(location, point) -- order by distance to point
$$ LANGUAGE 'sql';

-- SELECT * FROM point_to_address(ST_Transform(ST_SetSRID(ST_MakePoint(9.738889, 47.550535), 4326), 3857), 250) LIMIT 10;
