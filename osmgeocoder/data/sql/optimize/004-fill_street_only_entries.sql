-- update street only entries
UPDATE public.osm_struct_house h SET postcode = p.postcode
FROM public.osm_postal_code p
WHERE
	h.city = ''
	AND h.postcode = ''
	AND gis.ST_Within(h.geometry, p.geometry);