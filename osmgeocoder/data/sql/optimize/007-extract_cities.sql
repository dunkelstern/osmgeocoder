-- drop calculated tables
DROP TABLE IF EXISTS public.osm_struct_streets;
DROP TABLE IF EXISTS public.osm_struct_cities;

-- extract cities
SELECT
	crypto.gen_random_uuid() AS id,
	pc.name,
	pc.postcode,
	gis.ST_SetSRID(gis.ST_Extent(c.geometry), 3857) AS extent
INTO public.osm_struct_cities c
FROM public.osm_struct_house h
JOIN public.osm_struct_postcode pc ON
	(h.postcode = pc.postcode AND h.city = pc.name)
	OR gis.ST_Within(gis.ST_SetSRID(gis.ST_Extent(c.geometry), 3857), pc.area)
GROUP BY pc.name, pc.postcode, gis.ST_Extent(c.geometry);