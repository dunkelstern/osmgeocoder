-- copy table
DROP TABLE IF EXISTS public.osm_struct_house;
SELECT crypto.gen_random_uuid() AS id, osm_id, city, postcode, street, house_number, geometry INTO public.osm_struct_house FROM public.osm_house_number;

CREATE INDEX IF NOT EXISTS osm_buildings_house_number_idx ON public.osm_buildings USING BTREE(house_number);
ANALYZE public.osm_buildings;

INSERT INTO public.osm_struct_house (id, osm_id, city, postcode, street, house_number, geometry)
	SELECT crypto.gen_random_uuid() AS id, b.osm_id, '' AS city, p.postcode, b.street, b.house_number, gis.ST_Centroid(b.geometry) AS geometry
	FROM (SELECT * FROM public.osm_buildings WHERE house_number <> '') b
	JOIN public.osm_postal_code p ON gis.ST_Intersects(p.geometry, b.geometry);

CREATE INDEX osm_struct_house_city_idx ON public.osm_struct_house USING BTREE(city);
CREATE INDEX osm_struct_house_postcode_idx ON public.osm_struct_house USING BTREE(postcode);
CREATE INDEX osm_struct_house_street_idx ON public.osm_struct_house USING BTREE(street);

-- update street only entries
UPDATE public.osm_struct_house h SET postcode = p.postcode
FROM public.osm_postal_code p
WHERE
	h.city = ''
	AND h.postcode = ''
	AND gis.ST_Intersects(p.geometry, h.geometry);

-- update postcode only entries
UPDATE public.osm_struct_house h SET city = a.name
FROM public.osm_admin a
WHERE
	h.city = ''
	AND h.postcode <> ''
	AND a.admin_level = 8
	AND gis.ST_Intersects(a.geometry, h.geometry);

UPDATE public.osm_struct_house h SET city = a.name
FROM public.osm_admin a
WHERE
	h.city = ''
	AND h.postcode <> ''
	AND a.admin_level = 6
	AND gis.ST_Intersects(a.geometry, h.geometry);

-- drop calculated tables
DROP TABLE IF EXISTS public.osm_struct_streets;
DROP TABLE IF EXISTS public.osm_struct_cities;

-- extract cities
SELECT
	crypto.gen_random_uuid() AS id,
	city AS name,
	postcode,
	gis.ST_SetSRID(gis.ST_Extent(geometry), 3857) AS extent
INTO public.osm_struct_cities
FROM public.osm_struct_house
WHERE city <> '' OR postcode <> ''
GROUP BY city, postcode;


ALTER TABLE public.osm_struct_cities ADD PRIMARY KEY (id);

CREATE INDEX osm_struct_cities_name_idx ON public.osm_struct_cities USING BTREE(name);
CREATE INDEX osm_struct_cities_postcode_idx ON public.osm_struct_cities USING BTREE(postcode);
CREATE INDEX osm_struct_cities_extent_idx ON public.osm_struct_cities USING GIST(extent);

ALTER TABLE public.osm_struct_house ADD COLUMN city_id uuid REFERENCES public.osm_struct_cities (id);

UPDATE public.osm_struct_house h
	SET city_id = c.id
	FROM public.osm_struct_cities c
	WHERE
		h.city = c.name
		AND h.postcode = c.postcode;

CREATE INDEX osm_struct_house_city_id_idx ON public.osm_struct_house USING BTREE(city_id);

-- extract streets
SELECT
	crypto.gen_random_uuid() AS id,
	street AS name,
	city_id,
	gis.ST_SetSRID(gis.ST_Extent(geometry), 3857) AS extent
INTO public.osm_struct_streets
FROM public.osm_struct_house
GROUP BY city_id, street;

ALTER TABLE public.osm_struct_streets ADD PRIMARY KEY (id);

CREATE INDEX osm_struct_streets_name_idx ON public.osm_struct_streets USING BTREE(name);
CREATE INDEX osm_struct_streets_city_idx ON public.osm_struct_streets USING BTREE(city_id);
CREATE INDEX osm_struct_streets_extent_idx ON public.osm_struct_streets USING GIST(extent);

ALTER TABLE public.osm_struct_house ADD COLUMN street_id uuid REFERENCES public.osm_struct_streets (id);

UPDATE public.osm_struct_house h
	SET street_id = s.id
	FROM public.osm_struct_streets s
	WHERE
		s.city_id = h.city_id
		AND s.name = h.street;

CREATE INDEX osm_struct_house_street_id_idx ON public.osm_struct_house USING BTREE(street_id);

-- fetch geometry for street from osm_roads
ALTER TABLE public.osm_struct_streets ADD COLUMN geometry gis.geometry(linestring, 3857);

UPDATE public.osm_struct_streets s SET geometry = r.geometry
	FROM public.osm_roads r
	WHERE
		r.street = s.name
		AND gis.ST_Intersects(s.extent, gis.ST_SetSRID(gis.Box2D(r.geometry), 3857));

-- fetch geometry for city from osm_admin
ALTER TABLE public.osm_struct_cities ADD COLUMN geometry gis.geometry(geometry, 3857);

UPDATE public.osm_struct_cities c SET geometry = p.geometry
	FROM public.osm_postal_code p
	WHERE
		c.geometry IS NULL
		AND p.postcode = c.postcode
		AND gis.ST_Intersects(c.extent, gis.ST_SetSRID(gis.Box2D(p.geometry), 3857));

UPDATE public.osm_struct_cities c SET geometry = a.geometry
	FROM public.osm_admin a
	WHERE
		c.geometry IS NULL
		AND a.name = c.name
		AND a.admin_level = 8
		AND gis.ST_Intersects(c.extent, gis.ST_SetSRID(gis.Box2D(a.geometry), 3857));

-- clean up
ALTER TABLE public.osm_struct_house DROP COLUMN city, DROP COLUMN postcode, DROP COLUMN street, DROP COLUMN city_id;
ANALYZE public.osm_struct_cities;
ANALYZE public.osm_struct_house;
ANALYZE public.osm_struct_streets;

CREATE INDEX osm_struct_house_geohash_idx ON public.osm_struct_house USING BTREE(gis.ST_Geohash(gis.ST_Transform(geometry, 4326)));
CLUSTER public.osm_struct_house USING osm_struct_house_geohash_idx;

CREATE INDEX osm_struct_house_geometry ON public.osm_struct_house USING GIST(geometry);
CREATE INDEX osm_struct_street_geometry ON public.osm_struct_streets USING GIST(geometry);
CREATE INDEX osm_struct_city_geometry ON public.osm_struct_cities USING GIST(geometry);
