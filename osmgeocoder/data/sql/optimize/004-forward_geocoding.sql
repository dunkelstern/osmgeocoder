--
-- Function to query for a country polygon
-- used by country queries to avoid joining the admin table and recalculating the intersection
-- on every row of the result set, does a trigram search on the country name
--
DROP FUNCTION IF EXISTS _geocode_get_country_polygon(search_term TEXT);
CREATE FUNCTION _geocode_get_country_polygon(search_term TEXT) RETURNS geometry AS
$$
	SELECT geometry
	FROM osm_admin a
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
DROP FUNCTION IF EXISTS _geocode_by_road_without_country_osm(
    search_term TEXT, search_housenumber TEXT, max_results int,
    center geometry(point), radius int);
CREATE OR REPLACE FUNCTION _geocode_by_road_without_country_osm(
	search_term TEXT,
    search_housenumber TEXT,
	max_results int,
	center geometry(point),
	radius int
)
RETURNS SETOF address_and_distance
AS $$
    SELECT
        NULLIF(b.name, '')::text AS house,
        b.road::text,
        b.house_number::text,
        pc.postcode::text,
        NULLIF(a.name, '')::text as city,
        b.location::geometry(point),
        b.distance
    FROM (
        SELECT
            b.*,
            ST_Centroid(b.geometry) as location,
            ST_Distance(b.geometry, center) as distance
        FROM
            osm_buildings b
        WHERE
        	(center IS NULL OR ST_DWithin(b.geometry, center, radius))
            AND b.road % search_term
            AND (search_housenumber IS NULL OR b.house_number % search_housenumber)
        ORDER BY
        	distance ASC,
            (road <-> search_term) ASC
        LIMIT max_results
    ) b
    LEFT JOIN osm_postal_code pc
        ON ST_Within(b.location, pc.geometry)
    LEFT JOIN osm_admin a
        ON (a.admin_level = 6 AND ST_Within(b.location, a.geometry))
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
DROP FUNCTION IF EXISTS _geocode_by_road_with_country_osm(
    search_term TEXT, search_housenumber TEXT, max_results int,
    center geometry(point), radius int, country TEXT);
CREATE OR REPLACE FUNCTION _geocode_by_road_with_country_osm(
	search_term TEXT,
    search_housenumber TEXT,
	max_results int,
	center geometry(point),
	radius int,
	country TEXT
)
RETURNS SETOF address_and_distance AS
$$
DECLARE
	country_poly geometry;
BEGIN
    -- prefetch the country polyon to avoid doing a join in the query
	SELECT _geocode_get_country_polygon(country) INTO country_poly;
	
    RETURN QUERY SELECT
        NULLIF(b.name, '') AS house,
        b.road::text,
        b.house_number::text,
        pc.postcode::text,
        NULLIF(a.name, '')::text as city,
        b.location::geometry(point),
        b.distance
    FROM (
        SELECT
            b.*,
            road <-> search_term as trgm_dist, -- for sorting
            ST_Centroid(b.geometry) as location,
            ST_Distance(b.geometry, center) as distance -- secondary sorting
        FROM
            osm_buildings b
        WHERE
        	(center IS NULL OR ST_DWithin(b.geometry, center, radius)) -- only search around center if center is not null
        	AND ST_Within(ST_Centroid(b.geometry), country_poly) -- intersect with country polygon
            AND b.road % search_term -- trigram search for road name
            AND (search_housenumber IS NULL OR b.house_number % search_housenumber)
        ORDER BY
        	distance ASC,
            trgm_dist ASC
        LIMIT max_results -- limit here to avoid performing the joins on all rows
    ) b
    -- only join postal code and admin table on the limited resultset of the road query to avoid big joins
    LEFT JOIN osm_postal_code pc  
        ON ST_Within(b.locationm, pc.geometry)
    LEFT JOIN osm_admin a
        ON (a.admin_level = 6 AND ST_Within(b.location, a.geometry));
END;
$$ LANGUAGE 'plpgsql';

--
-- Convenience switching function that calls the correct detail function
--
-- This is the external interface to the forward geocoder
--
DROP FUNCTION IF EXISTS geocode_by_road_osm(
    search_term TEXT, search_housenumber TEXT, max_results int,
    center geometry(point), radius int, country TEXT
);
CREATE OR REPLACE FUNCTION geocode_by_road_osm(
	search_term TEXT,
    search_housenumber TEXT,
	max_results int,
	center geometry(point),
	radius int,
	country TEXT
)
RETURNS SETOF address_and_distance
AS $$
DECLARE
BEGIN
	IF country IS NULL THEN
        -- no country, use the simplified and fast functions
		RETURN QUERY SELECT * FROM _geocode_by_road_without_country_osm(
            search_term, search_housenumber, max_results, center, radius
        );
	ELSE
        -- have a country, use the more precise but slower functions
		RETURN QUERY SELECT * FROM _geocode_by_road_with_country_osm(
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
DROP FUNCTION IF EXISTS _geocode_by_city_without_country_osm(
    search_term TEXT, search_housenumber TEXT, search_city TEXT,
    max_results int, center geometry(point), radius int
);
CREATE OR REPLACE FUNCTION _geocode_by_city_without_country_osm(
	search_term TEXT,
    search_housenumber TEXT,
	search_city TEXT,
	max_results int,
	center geometry(point),
	radius int
)
RETURNS SETOF address_and_distance AS
$$
DECLARE
    poly geometry;
BEGIN
	SELECT ST_Union(ARRAY_AGG(geometry)) INTO poly FROM osm_admin WHERE
		"name" % search_city
        AND admin_level = 6
        AND (center IS NULL OR ST_DWithin(geometry, center, radius));


    RETURN QUERY SELECT
        NULLIF(b.name, '')::text AS house,
        b.road::text,
        b.house_number::text,
        pc.postcode::text,
        NULLIF(a.name, '')::text as city,
        b.location::geometry(point),
        b.distance
    FROM (
        SELECT
            b.*,
            road <-> search_term as trgm_dist,
            ST_Centroid(b.geometry) as location,
            ST_Distance(b.geometry, center) as distance -- secondary sorting
        FROM osm_buildings b
        WHERE
            (center IS NULL OR ST_DWithin(b.geometry, center, radius))
            AND ST_Within(ST_Centroid(b.geometry), poly)
            AND (b.road % search_term)
            AND (search_housenumber IS NULL OR b.house_number % search_housenumber)
        ORDER BY 
            distance ASC,
            trgm_dist ASC
        LIMIT max_results
    ) b
    LEFT JOIN osm_admin a
        ON ST_Within(b.location, a.geometry) AND a.admin_level = 6
    LEFT JOIN osm_postal_code pc
        ON ST_Within(b.location, pc.geometry);
END
$$ LANGUAGE 'plpgsql';


--
-- geocode by searching road-names in combination with a city
--
-- optionally only search in an area around `center` (with the `radius` specified)
-- this function is used when a country search term is supplied (e.g. country may not be NULL)
--
-- This query is a bit slower than just searching by center and radius as there will be a costly
-- intersection with the country polygon which can be rather large (5MB for germany for example)
--
DROP FUNCTION IF EXISTS _geocode_by_city_with_country_osm(
    search_term TEXT, search_housenumber TEXT, search_city TEXT,
    max_results int, center geometry(point), radius int, country TEXT
);
CREATE OR REPLACE FUNCTION _geocode_by_city_with_country_osm(
	search_term TEXT,
    search_housenumber TEXT,
    search_city TEXT,
	max_results int,
	center geometry(point),
	radius int,
	country TEXT
)
RETURNS SETOF address_and_distance AS
$$
DECLARE
    poly geometry;
    country_poly geometry;
BEGIN
	SELECT ST_Union(ARRAY_AGG(geometry)) INTO poly FROM osm_admin WHERE
		"name" % search_city
        AND admin_level = 6
        AND (center IS NULL OR ST_DWithin(geometry, center, radius));


    RETURN QUERY SELECT
        NULLIF(b.name, '')::text AS house,
        b.road::text,
        b.house_number::text,
        pc.postcode::text,
        NULLIF(a.name, '')::text as city,
        b.location::geometry(point),
        b.distance
    FROM (
        SELECT
            b.*,
            road <-> search_term as trgm_dist,
            ST_Centroid(b.geometry) as location,
            ST_Distance(b.geometry, center) as distance -- secondary sorting
        FROM osm_buildings b
        WHERE
            (center IS NULL OR ST_DWithin(b.geometry, center, radius))
            AND ST_Within(ST_Centroid(b.geometry), poly)
            AND ST_Within(ST_Centroid(b.geometry), country_poly) -- intersect with country polygon
            AND (b.road % search_term)
            AND (search_housenumber IS NULL OR b.house_number % search_housenumber)
        ORDER BY 
            distance ASC,
            trgm_dist ASC
        LIMIT max_results
    ) b
    LEFT JOIN osm_admin a
        ON ST_Within(b.location, a.geometry) AND a.admin_level = 6
    LEFT JOIN osm_postal_code pc
        ON ST_Within(b.location, pc.geometry);
END
$$ LANGUAGE 'plpgsql';
--
-- Convenience switching function that calls the correct detail function
--
-- This is the external interface to the forward geocoder
--
DROP FUNCTION IF EXISTS geocode_by_city_osm(
    search_term TEXT, search_housenumber TEXT, search_city TEXT,
    max_results int, center geometry(point), radius int, country TEXT
);
CREATE OR REPLACE FUNCTION geocode_by_city_osm(
	search_term TEXT,
    search_housenumber TEXT,
    search_city TEXT,
	max_results int,
	center geometry(point),
	radius int,
	country TEXT
)
RETURNS SETOF address_and_distance AS
$$
DECLARE
BEGIN
	IF country IS NULL THEN
        -- no country, use the simplified and fast functions
		RETURN QUERY SELECT * FROM _geocode_by_city_without_country_osm(search_term, search_housenumber, search_city, max_results, center, radius);
	ELSE
        -- have a country, use the more precise but slower functions
		RETURN QUERY SELECT * FROM _geocode_by_city_with_country_osm(search_term, search_housenumber, search_city, max_results, center, radius, country);
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
DROP FUNCTION IF EXISTS _geocode_by_postcode_without_country_osm(
    search_term TEXT, search_housenumber TEXT, search_postcode TEXT,
    max_results int, center geometry(point), radius int
);
CREATE OR REPLACE FUNCTION _geocode_by_postcode_without_country_osm(
	search_term TEXT,
    search_housenumber TEXT,
	search_postcode TEXT,
	max_results int,
	center geometry(point),
	radius int
)
RETURNS SETOF address_and_distance AS
$$
DECLARE
	poly geometry;
BEGIN
    -- prefetch and merge the postcode area
	SELECT ST_Union(ARRAY_AGG(geometry)) INTO poly FROM osm_postal_code WHERE
		postcode like search_postcode || '%'
        AND (center IS NULL OR ST_DWithin(geometry, center, radius));
	
	RETURN QUERY SELECT
	  b.house::text,
	  b.road::text,
	  b.house_number::text,
	  pc.postcode::text,
      NULLIF(a.name, '')::text as city,
      b.location::geometry(point),
      b.distance
	FROM (
	    SELECT
	        NULLIF(b.name, '') as house,
	        b.road,
	        b.house_number,
	        ST_Centroid(b.geometry) as location,
	        ST_Distance(b.geometry, center) as distance -- secondary sorting
	    FROM osm_buildings b
	    WHERE
	        (center IS NULL OR ST_DWithin(b.geometry, center, radius))
	   		AND ST_Within(b.geometry, poly)
	        AND (b.road % search_term)
            AND (search_housenumber IS NULL OR b.house_number % search_housenumber)
	    ORDER BY
	        distance ASC,
	        (road <-> search_term) ASC
	    LIMIT max_results
   	) b
    -- late joins to avoid large overhead
	LEFT JOIN osm_admin a
		ON (a.admin_level = 6 AND ST_Within(b.location, a.geometry))
    LEFT JOIN osm_postal_code pc
        ON ST_Within(b.location, pc.geometry);
END;
$$ LANGUAGE 'plpgsql';


--
-- geocode by searching road-names in combination with a postcode
--
-- optionally only search in an area around `center` (with the `radius` specified)
-- this function is used when no country search term is supplied
--
-- This query is a bit slower than just searching by center and radius as there will be a costly
-- intersection with the country polygon which can be rather large (5MB for germany for example)
--
DROP FUNCTION IF EXISTS _geocode_by_postcode_with_country_osm(
    search_term TEXT, search_housenumber TEXT, search_postcode TEXT,
    max_results int, center geometry(point), radius int, country TEXT
);
CREATE OR REPLACE FUNCTION _geocode_by_postcode_with_country_osm(
	search_term TEXT,
    search_housenumber TEXT,
	search_postcode TEXT,
	max_results int,
	center geometry(point),
	radius int,
    country TEXT
)
RETURNS SETOF address_and_distance AS
$$
DECLARE
	poly geometry;
    country_poly geometry;
BEGIN
    -- prefetch the country polyon to avoid doing a join in the query
	SELECT _geocode_get_country_polygon(country) INTO country_poly;

	SELECT ST_Union(ARRAY_AGG(geometry)) INTO poly FROM osm_postal_code WHERE
		postcode like search_postcode || '%'
        AND (center IS NULL OR ST_DWithin(geometry, center, radius));
	
	RETURN QUERY SELECT
	  b.house::text,
	  b.road::text,
	  b.house_number::text,
	  pc.postcode::text,
      NULLIF(a.name, '')::text as city,
      b.location::geometry(point),
      b.distance
	FROM (
	    SELECT
	        NULLIF(b.name, '') as house,
	        b.road,
	        b.house_number,
	        ST_Centroid(b.geometry) as location,
	        ST_Distance(b.geometry, center) as distance -- secondary sorting
	    FROM osm_buildings b
	    WHERE
	        (center IS NULL OR ST_DWithin(b.geometry, center, radius))
	   		AND ST_Within(ST_Centroid(b.geometry), poly)
            AND ST_Within(ST_Centroid(b.geometry), country_poly) -- intersect with country polygon
	        AND (b.road % search_term)
            AND (search_housenumber IS NULL OR b.house_number % search_housenumber)
	    ORDER BY
	        distance ASC,
	        (road <-> search_term) ASC
	    LIMIT max_results
   	) b
    -- late joins to avoid large overhead
	LEFT JOIN osm_admin a
		ON (a.admin_level = 6 AND ST_Within(b.location, a.geometry))
    LEFT JOIN osm_postal_code pc
        ON ST_Within(b.location, pc.geometry);
END;
$$ LANGUAGE 'plpgsql';


--
-- Convenience switching function that calls the correct detail function
--
-- This is the external interface to the forward geocoder
--
DROP FUNCTION IF EXISTS geocode_by_postcode_osm(
    search_term TEXT, search_housenumber TEXT, search_postcode TEXT,
    max_results int, center geometry(point), radius int, country TEXT
);
CREATE OR REPLACE FUNCTION geocode_by_postcode_osm(
	search_term TEXT,
    search_housenumber TEXT,
    search_postcode TEXT,
	max_results int,
	center geometry(point),
	radius int,
	country TEXT
)
RETURNS SETOF address_and_distance AS
$$
DECLARE
BEGIN
	IF country IS NULL THEN
        -- no country, use the simplified and fast functions
		RETURN QUERY SELECT * FROM _geocode_by_postcode_without_country_osm(search_term, search_housenumber, search_postcode, max_results, center, radius);
	ELSE
        -- have a country, use the more precise but slower functions
		RETURN QUERY SELECT * FROM _geocode_by_postcode_with_country_osm(search_term, search_housenumber, search_postcode, max_results, center, radius, country);
	END IF;
END;
$$ LANGUAGE 'plpgsql';


--
-- Convenience switching function that calls the correct detail function
--
-- This is the external interface to the forward geocoder
--
DROP FUNCTION IF EXISTS geocode_osm(
    search_term TEXT, search_housenumber TEXT, search_postcode TEXT,
    search_city TEXT, max_results int, center geometry(point),
    radius int, country TEXT
);
CREATE OR REPLACE FUNCTION geocode_osm(
	search_term TEXT,
    search_housenumber TEXT,
    search_postcode TEXT,
    search_city TEXT,
	max_results int,
	center geometry(point),
	radius int,
	country TEXT
)
RETURNS SETOF address_and_distance AS
$$
DECLARE
BEGIN
    IF search_postcode IS NOT NULL THEN
        RETURN QUERY SELECT * FROM geocode_by_postcode_osm(
            search_term, search_housenumber, search_postcode, max_results,
            center, radius, country
        );
        RETURN;
    END IF;
    IF search_city IS NOT NULL THEN
        RETURN QUERY SELECT * FROM geocode_by_city_osm(
            search_term, search_housenumber, search_city, max_results,
            center, radius, country
        );
        RETURN;
    END IF;

    RETURN QUERY SELECT * FROM geocode_by_road_osm(
        search_term, search_housenumber, max_results, center, radius,
        country
    );
END;
$$ LANGUAGE 'plpgsql';

-- SELECT * FROM geocode_osm('Georgenstr', '34', NULL, 'Amberg', 10, NULL, NULL, NULL);