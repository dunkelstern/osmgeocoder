ALTER TABLE public.osm_struct_house ADD COLUMN city_id uuid REFERENCES public.osm_struct_cities (id);

UPDATE public.osm_struct_house h
	SET city_id = c.id
	FROM public.osm_struct_cities c
	WHERE
		h.city = c.name
		AND h.postcode = c.postcode;

CREATE INDEX osm_struct_house_city_id_idx ON public.osm_struct_house USING BTREE(city_id);
ANALYZE public.osm_struct_house;