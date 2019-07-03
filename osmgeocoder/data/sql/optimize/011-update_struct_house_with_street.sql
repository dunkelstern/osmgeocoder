UPDATE public.osm_struct_house h
	SET street_id = s.id
	FROM public.osm_struct_streets s
	WHERE
		s.city_id = h.city_id
		AND s.name = h.street;

CREATE INDEX osm_struct_house_street_id_idx ON public.osm_struct_house USING BTREE(street_id);