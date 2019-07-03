-- fetch geometry for street from osm_roads
ALTER TABLE public.osm_struct_streets ADD COLUMN geometry gis.geometry(linestring, 3857);

UPDATE public.osm_struct_streets s SET geometry = r.geometry
	FROM public.osm_roads r
	WHERE
		r.street = s.name
		AND gis.ST_Intersects(s.extent, gis.ST_SetSRID(gis.Box2D(r.geometry), 3857));
