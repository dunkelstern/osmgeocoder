-- update postcode only entries
UPDATE public.osm_struct_house h SET city = a.name
FROM public.osm_admin a
WHERE
	h.city = ''
	AND h.postcode <> ''
	AND a.admin_level = 8
	AND gis.ST_Within(h.geometry, a.geometry);

UPDATE public.osm_struct_house h SET city = a.name
FROM public.osm_admin a
WHERE
	h.city = ''
	AND h.postcode <> ''
	AND a.admin_level = 6
	AND gis.ST_Within(h.geometry, a.geometry);