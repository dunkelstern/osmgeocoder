ALTER TABLE public.osm_struct_streets ADD PRIMARY KEY (id);
ALTER TABLE public.osm_struct_streets ADD CONSTRAINT osm_struct_streets_city_id_fk FOREIGN KEY (city_id) REFERENCES public.osm_struct_cities(id);

CREATE INDEX osm_struct_streets_name_idx ON public.osm_struct_streets USING BTREE(name);
CREATE INDEX osm_struct_streets_city_idx ON public.osm_struct_streets USING BTREE(city_id);
CREATE INDEX osm_struct_streets_extent_idx ON public.osm_struct_streets USING GIST(extent);