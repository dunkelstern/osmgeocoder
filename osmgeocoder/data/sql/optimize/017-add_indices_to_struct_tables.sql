CREATE INDEX osm_struct_house_street_id_idx ON public.osm_struct_house USING BTREE(street_id);

CREATE INDEX osm_struct_house_geometry ON public.osm_struct_house USING GIST(geometry);
CREATE INDEX osm_struct_street_geometry ON public.osm_struct_streets USING GIST(geometry);
CREATE INDEX osm_struct_city_geometry ON public.osm_struct_cities USING GIST(geometry);

ANALYZE public.osm_struct_house;
ANALYZE public.osm_struct_streets;
ANALYZE public.osm_struct_cities;