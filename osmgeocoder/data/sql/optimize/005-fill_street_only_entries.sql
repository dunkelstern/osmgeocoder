-- update street only entries
UPDATE public.osm_struct_house h 
SET 
	postcode = p.postcode,
	city = p.name 
FROM public.osm_struct_postcode p
WHERE
	h.city = ''
	AND h.postcode = ''
	AND gis.ST_Within(h.geometry, p.area);