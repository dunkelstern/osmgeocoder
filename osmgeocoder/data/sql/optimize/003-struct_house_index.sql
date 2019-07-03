CREATE INDEX osm_struct_house_city_idx ON public.osm_struct_house USING BTREE(city);
CREATE INDEX osm_struct_house_postcode_idx ON public.osm_struct_house USING BTREE(postcode);
CREATE INDEX osm_struct_house_street_idx ON public.osm_struct_house USING BTREE(street);

ANALYZE public.osm_struct_house;