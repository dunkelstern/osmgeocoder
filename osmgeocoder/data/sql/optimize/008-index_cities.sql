ALTER TABLE public.osm_struct_cities ADD PRIMARY KEY (id);

CREATE INDEX osm_struct_cities_name_idx ON public.osm_struct_cities USING BTREE(name);
CREATE INDEX osm_struct_cities_postcode_idx ON public.osm_struct_cities USING BTREE(postcode);
CREATE INDEX osm_struct_cities_extent_idx ON public.osm_struct_cities USING GIST(extent);

ANALYZE public.osm_struct_cities;

