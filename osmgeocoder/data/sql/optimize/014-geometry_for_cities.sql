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