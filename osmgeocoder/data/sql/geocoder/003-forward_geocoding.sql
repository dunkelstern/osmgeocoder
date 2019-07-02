--
-- Function to query for a country polygon
-- used by country queries to avoid joining the admin table and recalculating the intersection
-- on every row of the result set, does a trigram search on the country name
--
DROP FUNCTION IF EXISTS public._geocode_get_country_polygon(search_term TEXT);
CREATE FUNCTION public._geocode_get_country_polygon(search_term TEXT) RETURNS gis.geometry AS
$$
	SELECT geometry
	FROM public.osm_admin a
   	WHERE 
   		a.admin_level = 2 
   		AND a.name % search_term
$$ LANGUAGE 'sql' IMMUTABLE;


--
-- geocode by searching road-names only
--
-- optionally only search in an area around `center` (with the `radius` specified)
-- this function is used when no country search term is supplied
--
-- This query is quicker than matching against the country polygon additionally, but can be
-- imprecise when the address is near a country border
--
DROP FUNCTION IF EXISTS public._geocode_by_road_without_country_osm(
    search_term TEXT, search_housenumber TEXT, max_results int,
    center gis.geometry(point), radius int);
CREATE OR REPLACE FUNCTION public._geocode_by_road_without_country_osm(
	search_term TEXT,
    search_housenumber TEXT,
	max_results int,
	center gis.geometry(point),
	radius int
)
RETURNS SETOF public.address_and_distance
AS $$
    SELECT
        NULL::text AS house,
        s.name::text as road,
        h.house_number::text,
        c.postcode::text,
        NULLIF(c.name, '')::text as city,
        h.geometry::gis.geometry(point, 3857),
        gis.ST_Distance(h.geometry, center) as distance,
        '00000000-0000-0000-0000-000000000000'::uuid as license_id
    FROM
        public.osm_struct_streets s
    JOIN public.osm_struct_cities c ON s.city_id = c.id
    JOIN public.osm_struct_house h ON h.street_id = s.id
    WHERE
        (center IS NULL OR gis.ST_DWithin(h.geometry, center, radius)) -- only search around center if center is not null
        AND s.name % search_term
        AND (search_housenumber IS NULL OR h.house_number % search_housenumber)
    ORDER BY
        distance ASC,
        (s.name <-> search_term) ASC
    LIMIT max_results;
$$ LANGUAGE 'sql';

--
-- geocode by searching road-names only
--
-- optionally only search in an area around `center` (with the `radius` specified)
-- this function is used when a country search term is supplied (e.g. country may not be NULL)
--
-- This query is a bit slower than just searching by center and radius as there will be a costly
-- intersection with the country polygon which can be rather large (5MB for germany for example)
--
DROP FUNCTION IF EXISTS public._geocode_by_road_with_country_osm(
    search_term TEXT, search_housenumber TEXT, max_results int,
    center gis.geometry(point), radius int, country TEXT);
CREATE OR REPLACE FUNCTION _geocode_by_road_with_country_osm(
	search_term TEXT,
    search_housenumber TEXT,
	max_results int,
	center gis.geometry(point),
	radius int,
	country TEXT
)
RETURNS SETOF public.address_and_distance AS
$$
DECLARE
	country_poly gis.geometry;
BEGIN
    -- prefetch the country polyon to avoid doing a join in the query
	SELECT public._geocode_get_country_polygon(country) INTO country_poly;
	
    RETURN QUERY SELECT
        NULL::text AS house,
        s.name::text as road,
        h.house_number::text,
        c.postcode::text,
        NULLIF(c.name, '')::text as city,
        h.geometry::gis.geometry(point, 3857),
        gis.ST_Distance(h.geometry, center) as distance,
        '00000000-0000-0000-0000-000000000000'::uuid as license_id
    FROM
        public.osm_struct_streets s
    JOIN public.osm_struct_cities c ON s.city_id = c.id
    JOIN public.osm_struct_house h ON h.street_id = s.id
    WHERE
        (center IS NULL OR gis.ST_DWithin(h.geometry, center, radius)) -- only search around center if center is not null
        AND gis.ST_Within(gis.ST_Centroid(b.geometry), country_poly) -- intersect with country polygon
        AND s.name % search_term
        AND (search_housenumber IS NULL OR h.house_number % search_housenumber)
    ORDER BY
        distance ASC,
        (s.name <-> search_term) ASC
    LIMIT max_results; -- limit here to avoid performing the joins on all rows
END;
$$ LANGUAGE 'plpgsql';

--
-- Convenience switching function that calls the correct detail function
--
-- This is the external interface to the forward geocoder
--
DROP FUNCTION IF EXISTS public.geocode_by_road_osm(
    search_term TEXT, search_housenumber TEXT, max_results int,
    center gis.geometry(point), radius int, country TEXT
);
CREATE OR REPLACE FUNCTION public.geocode_by_road_osm(
	search_term TEXT,
    search_housenumber TEXT,
	max_results int,
	center gis.geometry(point),
	radius int,
	country TEXT
)
RETURNS SETOF public.address_and_distance
AS $$
DECLARE
BEGIN
	IF country IS NULL THEN
        -- no country, use the simplified and fast functions
		RETURN QUERY SELECT * FROM public._geocode_by_road_without_country_osm(
            search_term, search_housenumber, max_results, center, radius
        );
	ELSE
        -- have a country, use the more precise but slower functions
		RETURN QUERY SELECT * FROM public._geocode_by_road_with_country_osm(
            search_term, search_housenumber, max_results, center, radius,
            country
        );
	END IF;
END;
$$ LANGUAGE 'plpgsql';


--
-- geocode by searching road-names in combination with a city
--
-- optionally only search in an area around `center` (with the `radius` specified)
-- this function is used when no country search term is supplied
--
-- This query is quicker than matching against the country polygon additionally, but can be
-- imprecise when the address is near a country border
--
DROP FUNCTION IF EXISTS public._geocode_by_city_without_country_osm(
    search_term TEXT, search_housenumber TEXT, search_city TEXT,
    max_results int, center gis.geometry(point), radius int
);
CREATE OR REPLACE FUNCTION public._geocode_by_city_without_country_osm(
	search_term TEXT,
    search_housenumber TEXT,
	search_city TEXT,
	max_results int,
	center gis.geometry(point),
	radius int
)
RETURNS SETOF public.address_and_distance AS
$$
    SELECT
        NULL::text AS house,
        s.name::text as road,
        h.house_number::text,
        c.postcode::text,
        NULLIF(c.name, '')::text as city,
        h.geometry::gis.geometry(point, 3857),
        gis.ST_Distance(h.geometry, center) as distance,
        '00000000-0000-0000-0000-000000000000'::uuid as license_id
    FROM
        public.osm_struct_streets s
    JOIN public.osm_struct_cities c ON s.city_id = c.id
    JOIN public.osm_struct_house h ON h.street_id = s.id
    WHERE
        (center IS NULL OR gis.ST_DWithin(h.geometry, center, radius)) -- only search around center if center is not null
        AND c.name % search_city
        AND s.name % search_term
        AND (search_housenumber IS NULL OR h.house_number % search_housenumber)
    ORDER BY
        distance ASC,
        (s.name <-> search_term) ASC
    LIMIT max_results;
$$ LANGUAGE 'sql';


--
-- geocode by searching road-names in combination with a city
--
-- optionally only search in an area around `center` (with the `radius` specified)
-- this function is used when a country search term is supplied (e.g. country may not be NULL)
--
-- This query is a bit slower than just searching by center and radius as there will be a costly
-- intersection with the country polygon which can be rather large (5MB for germany for example)
--
DROP FUNCTION IF EXISTS public._geocode_by_city_with_country_osm(
    search_term TEXT, search_housenumber TEXT, search_city TEXT,
    max_results int, center gis.geometry(point), radius int, country TEXT
);
CREATE OR REPLACE FUNCTION public._geocode_by_city_with_country_osm(
	search_term TEXT,
    search_housenumber TEXT,
    search_city TEXT,
	max_results int,
	center gis.geometry(point),
	radius int,
	country TEXT
)
RETURNS SETOF public.address_and_distance AS
$$
DECLARE
    country_poly gis.geometry;
BEGIN
    -- prefetch the country polyon to avoid doing a join in the query
	SELECT public._geocode_get_country_polygon(country) INTO country_poly;

    RETURN QUERY SELECT
        NULL::text AS house,
        s.name::text as road,
        h.house_number::text,
        c.postcode::text,
        NULLIF(c.name, '')::text as city,
        h.geometry::gis.geometry(point, 3857),
        gis.ST_Distance(h.geometry, center) as distance,
        '00000000-0000-0000-0000-000000000000'::uuid as license_id
    FROM
        public.osm_struct_streets s
    JOIN public.osm_struct_cities c ON s.city_id = c.id
    JOIN public.osm_struct_house h ON h.street_id = s.id
    WHERE
        (center IS NULL OR gis.ST_DWithin(h.geometry, center, radius)) -- only search around center if center is not null
        AND gis.ST_Within(gis.ST_Centroid(b.geometry), country_poly) -- intersect with country polygon
        AND c.name % search_city
        AND s.name % search_term
        AND (search_housenumber IS NULL OR h.house_number % search_housenumber)
    ORDER BY
        distance ASC,
        (s.name <-> search_term) ASC
    LIMIT max_results;
END
$$ LANGUAGE 'plpgsql';

--
-- Convenience switching function that calls the correct detail function
--
-- This is the external interface to the forward geocoder
--
DROP FUNCTION IF EXISTS public.geocode_by_city_osm(
    search_term TEXT, search_housenumber TEXT, search_city TEXT,
    max_results int, center gis.geometry(point), radius int, country TEXT
);
CREATE OR REPLACE FUNCTION public.geocode_by_city_osm(
	search_term TEXT,
    search_housenumber TEXT,
    search_city TEXT,
	max_results int,
	center gis.geometry(point),
	radius int,
	country TEXT
)
RETURNS SETOF public.address_and_distance AS
$$
DECLARE
BEGIN
	IF country IS NULL THEN
        -- no country, use the simplified and fast functions
		RETURN QUERY SELECT * FROM public._geocode_by_city_without_country_osm(search_term, search_housenumber, search_city, max_results, center, radius);
	ELSE
        -- have a country, use the more precise but slower functions
		RETURN QUERY SELECT * FROM public._geocode_by_city_with_country_osm(search_term, search_housenumber, search_city, max_results, center, radius, country);
	END IF;
END;
$$ LANGUAGE 'plpgsql';


--
-- geocode by searching road-names in combination with a postcode
--
-- optionally only search in an area around `center` (with the `radius` specified)
-- this function is used when no country search term is supplied
--
-- This query is quicker than matching against the country polygon additionally, but can be
-- imprecise when the address is near a country border
--
DROP FUNCTION IF EXISTS public._geocode_by_postcode_without_country_osm(
    search_term TEXT, search_housenumber TEXT, search_postcode TEXT,
    max_results int, center gis.geometry(point), radius int
);
CREATE OR REPLACE FUNCTION public._geocode_by_postcode_without_country_osm(
	search_term TEXT,
    search_housenumber TEXT,
	search_postcode TEXT,
	max_results int,
	center gis.geometry(point),
	radius int
)
RETURNS SETOF public.address_and_distance AS
$$
    SELECT
        NULL::text AS house,
        s.name::text as road,
        h.house_number::text,
        c.postcode::text,
        NULLIF(c.name, '')::text as city,
        h.geometry::gis.geometry(point, 3857),
        gis.ST_Distance(h.geometry, center) as distance,
        '00000000-0000-0000-0000-000000000000'::uuid as license_id
    FROM
        public.osm_struct_streets s
    JOIN public.osm_struct_cities c ON s.city_id = c.id
    JOIN public.osm_struct_house h ON h.street_id = s.id
    WHERE
        (center IS NULL OR gis.ST_DWithin(h.geometry, center, radius)) -- only search around center if center is not null
        AND s.name % search_term
        AND c.postcode % search_postcode
        AND (search_housenumber IS NULL OR h.house_number % search_housenumber)
    ORDER BY
        distance ASC,
        (s.name <-> search_term) ASC
    LIMIT max_results;
$$ LANGUAGE 'sql';


--
-- geocode by searching road-names in combination with a postcode
--
-- optionally only search in an area around `center` (with the `radius` specified)
-- this function is used when no country search term is supplied
--
-- This query is a bit slower than just searching by center and radius as there will be a costly
-- intersection with the country polygon which can be rather large (5MB for germany for example)
--
DROP FUNCTION IF EXISTS public._geocode_by_postcode_with_country_osm(
    search_term TEXT, search_housenumber TEXT, search_postcode TEXT,
    max_results int, center gis.geometry(point), radius int, country TEXT
);
CREATE OR REPLACE FUNCTION public._geocode_by_postcode_with_country_osm(
	search_term TEXT,
    search_housenumber TEXT,
	search_postcode TEXT,
	max_results int,
	center gis.geometry(point),
	radius int,
    country TEXT
)
RETURNS SETOF public.address_and_distance AS
$$
DECLARE
    country_poly geometry;
BEGIN
    -- prefetch the country polyon to avoid doing a join in the query
	SELECT public._geocode_get_country_polygon(country) INTO country_poly;

    RETURN QUERY SELECT
        NULL::text AS house,
        s.name::text as road,
        h.house_number::text,
        c.postcode::text,
        NULLIF(c.name, '')::text as city,
        h.geometry::gis.geometry(point, 3857),
        gis.ST_Distance(h.geometry, center) as distance,
        '00000000-0000-0000-0000-000000000000'::uuid as license_id
    FROM
        public.osm_struct_streets s
    JOIN public.osm_struct_cities c ON s.city_id = c.id
    JOIN public.osm_struct_house h ON h.street_id = s.id
    WHERE
        (center IS NULL OR gis.ST_DWithin(h.geometry, center, radius)) -- only search around center if center is not null
        AND gis.ST_Within(gis.ST_Centroid(b.geometry), country_poly) -- intersect with country polygon
        AND s.name % search_term
        AND c.postcode % search_postcode
        AND (search_housenumber IS NULL OR h.house_number % search_housenumber)
    ORDER BY
        distance ASC,
        (s.name <-> search_term) ASC
    LIMIT max_results;
END;
$$ LANGUAGE 'plpgsql';


--
-- Convenience switching function that calls the correct detail function
--
-- This is the external interface to the forward geocoder
--
DROP FUNCTION IF EXISTS public.geocode_by_postcode_osm(
    search_term TEXT, search_housenumber TEXT, search_postcode TEXT,
    max_results int, center gis.geometry(point), radius int, country TEXT
);
CREATE OR REPLACE FUNCTION public.geocode_by_postcode_osm(
	search_term TEXT,
    search_housenumber TEXT,
    search_postcode TEXT,
	max_results int,
	center gis.geometry(point),
	radius int,
	country TEXT
)
RETURNS SETOF public.address_and_distance AS
$$
DECLARE
BEGIN
	IF country IS NULL THEN
        -- no country, use the simplified and fast functions
		RETURN QUERY SELECT * FROM public._geocode_by_postcode_without_country_osm(search_term, search_housenumber, search_postcode, max_results, center, radius);
	ELSE
        -- have a country, use the more precise but slower functions
		RETURN QUERY SELECT * FROM public._geocode_by_postcode_with_country_osm(search_term, search_housenumber, search_postcode, max_results, center, radius, country);
	END IF;
END;
$$ LANGUAGE 'plpgsql';


--
-- Convenience switching function that calls the correct detail function
--
-- This is the external interface to the forward geocoder
--
DROP FUNCTION IF EXISTS public.geocode_osm(
    search_term TEXT, search_housenumber TEXT, search_postcode TEXT,
    search_city TEXT, max_results int, center gis.geometry(point),
    radius int, country TEXT
);
CREATE OR REPLACE FUNCTION public.geocode_osm(
	search_term TEXT,
    search_housenumber TEXT,
    search_postcode TEXT,
    search_city TEXT,
	max_results int,
	center gis.geometry(point),
	radius int,
	country TEXT
)
RETURNS SETOF public.address_and_distance AS
$$
DECLARE
BEGIN
    IF search_postcode IS NOT NULL THEN
        RETURN QUERY SELECT * FROM public.geocode_by_postcode_osm(
            search_term, search_housenumber, search_postcode, max_results,
            center, radius, country
        );
        RETURN;
    END IF;
    IF search_city IS NOT NULL THEN
        RETURN QUERY SELECT * FROM public.geocode_by_city_osm(
            search_term, search_housenumber, search_city, max_results,
            center, radius, country
        );
        RETURN;
    END IF;

    RETURN QUERY SELECT * FROM public.geocode_by_road_osm(
        search_term, search_housenumber, max_results, center, radius,
        country
    );
END;
$$ LANGUAGE 'plpgsql';

-- SELECT * FROM geocode_osm('Georgenstr', '34', NULL, 'Amberg', 10, NULL, NULL, NULL);