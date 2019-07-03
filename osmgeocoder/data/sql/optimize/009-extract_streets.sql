-- extract streets
SELECT
	crypto.gen_random_uuid() AS id,
	street AS name,
	city_id,
	gis.ST_SetSRID(gis.ST_Extent(geometry), 3857) AS extent
INTO public.osm_struct_streets
FROM public.osm_struct_house
GROUP BY city_id, street;