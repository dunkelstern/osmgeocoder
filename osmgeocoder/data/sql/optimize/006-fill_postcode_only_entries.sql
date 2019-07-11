-- update postcode only entries
UPDATE public.osm_struct_house h SET city = p.name
FROM public.osm_struct_postcode p
WHERE
	h.city = ''
	AND h.postcode = p.postcode
	AND gis.ST_Within(h.geometry, p.area);
