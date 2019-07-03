-- copy table
DROP TABLE IF EXISTS public.osm_struct_house;
SELECT crypto.gen_random_uuid() AS id, osm_id, city, postcode, street, house_number, geometry INTO public.osm_struct_house FROM public.osm_house_number;

CREATE INDEX IF NOT EXISTS osm_buildings_house_number_idx ON public.osm_buildings USING BTREE(house_number);
ANALYZE public.osm_buildings;