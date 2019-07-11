-- drop indices for performance while clustering
DROP INDEX IF EXISTS osm_struct_house_postcode_idx;
DROP INDEX IF EXISTS osm_struct_house_city_id_idx;
DROP INDEX IF EXISTS osm_struct_house_street_id_idx;

CREATE INDEX osm_struct_house_geohash_idx ON public.osm_struct_house USING BTREE(gis.ST_Geohash(gis.ST_Transform(geometry, 4326)));
CLUSTER public.osm_struct_house USING osm_struct_house_geohash_idx;

