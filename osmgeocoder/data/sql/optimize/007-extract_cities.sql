-- drop calculated tables
DROP TABLE IF EXISTS public.osm_struct_streets;
DROP TABLE IF EXISTS public.osm_struct_cities;

-- extract cities
SELECT
	crypto.gen_random_uuid() AS id,
	h.city as name,
	h.postcode,
	gis.ST_SetSRID(gis.ST_Extent(h.geometry), 3857) AS extent
INTO public.osm_struct_cities
FROM public.osm_struct_house h
GROUP BY h.city, h.postcode;