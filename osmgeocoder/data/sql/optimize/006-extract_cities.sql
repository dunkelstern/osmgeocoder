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