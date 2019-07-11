-- clean up
ALTER TABLE public.osm_struct_house DROP COLUMN city, DROP COLUMN postcode, DROP COLUMN street, DROP COLUMN city_id;
ANALYZE public.osm_struct_cities;
ANALYZE public.osm_struct_house;
ANALYZE public.osm_struct_streets;