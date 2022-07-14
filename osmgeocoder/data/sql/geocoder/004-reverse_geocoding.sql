DO
$$
DECLARE
	oa_exists boolean;
	osm_exists boolean;
BEGIN
    SELECT EXISTS (
        SELECT 1
        FROM   information_schema.tables 
        WHERE  table_schema = 'public'
        AND    table_name = 'oa_city'
    ) INTO oa_exists;

    SELECT EXISTS (
        SELECT 1
        FROM   information_schema.tables 
        WHERE  table_schema = 'public'
        AND    table_name = 'osm_struct_cities'
    ) INTO osm_exists;

	IF oa_exists THEN 
		--
		-- Geocode a point to the nearest address
		-- This is the Openaddresses.io version for finding an address.
		--
		DROP FUNCTION IF EXISTS public.point_to_address_oa(point gis.geometry(point), radius float);
		CREATE OR REPLACE FUNCTION public.point_to_address_oa(point gis.geometry(point), radius float)
		RETURNS SETOF public.address_and_distance AS
		$func$
			SELECT
				h.name AS house,
				s.street as road,
				h.housenumber as house_number,
				c.postcode,
				c.city,
				NULL as county,
				NULL as "state",
				location,
				gis.ST_Distance(location, point) as distance,
				c.license_id
			FROM public.oa_house h
			JOIN public.oa_street s ON h.street_id = s.id
			JOIN public.oa_city c ON s.city_id = c.id
			WHERE
				gis.ST_X(location) >= gis.ST_X(point) - radius
				AND gis.ST_X(location) <= gis.ST_X(point) + radius
				AND gis.ST_DWithin(location, point, radius) -- only search within radius
			ORDER BY gis.ST_Distance(location, point) -- order by distance to point
		$func$ LANGUAGE 'sql';
	ELSE
		DROP FUNCTION IF EXISTS public.point_to_address_oa(point gis.geometry(point), radius float);
		CREATE OR REPLACE FUNCTION public.point_to_address_oa(point gis.geometry(point), radius float)
		RETURNS SETOF public.address_and_distance AS
		$func$
			SELECT NULL::public.address_and_distance LIMIT 0; -- return an empty set
		$func$ LANGUAGE 'sql';
	END IF;

	IF osm_exists THEN
		--
		-- Geocode a point to the nearest address
		-- This is the OpenStreetMap version for finding an address.
		--
		DROP FUNCTION IF EXISTS public.point_to_address_osm(point gis.geometry(point), radius float);
		CREATE OR REPLACE FUNCTION public.point_to_address_osm(point gis.geometry(point), radius float)
		RETURNS SETOF public.address_and_distance AS
		$func$
			SELECT
				NULL::text AS house,
				s.name as road,
				h.house_number,
				c.postcode,
				c.name as city,
        		NULLIF(a6.name, '')::text as county,
        		NULLIF(a4.name, '')::text as "state",
				h.geometry as location,
				gis.ST_Distance(h.geometry, point) as distance,
				'00000000-0000-0000-0000-000000000000'::uuid as license_id
			FROM public.osm_struct_house h
			JOIN public.osm_struct_streets s ON h.street_id = s.id
			JOIN public.osm_struct_cities c ON s.city_id = c.id
		    LEFT JOIN public.osm_admin a4 ON gis.ST_Contains(a4.geometry, h.geometry::gis.geometry(point, 3857)) and a4.admin_level = 4
    		LEFT JOIN public.osm_admin a6 ON gis.ST_Contains(a6.geometry, h.geometry::gis.geometry(point, 3857)) and a6.admin_level = 6
			WHERE
				gis.ST_X(h.geometry) >= gis.ST_X(point) - radius
				AND gis.ST_X(h.geometry) <= gis.ST_X(point) + radius
				AND gis.ST_DWithin(h.geometry, point, radius) -- only search within radius
			ORDER BY gis.ST_Distance(h.geometry, point) -- order by distance to point
		$func$ LANGUAGE 'sql';
	ELSE
		DROP FUNCTION IF EXISTS public.point_to_address_osm(point gis.geometry(point), radius float);
		CREATE OR REPLACE FUNCTION public.point_to_address_osm(point gis.geometry(point), radius float)
		RETURNS SETOF public.address_and_distance AS
		$func$
			SELECT NULL::public.address_and_distance LIMIT 0; -- return an empty set
		$func$ LANGUAGE 'sql';
	END IF;
END;
$$ LANGUAGE 'plpgsql';

-- SELECT * FROM point_to_address_osm(ST_Transform(ST_SetSRID(ST_MakePoint(9.738889, 47.550535), 4326), 3857), 250) LIMIT 10;
