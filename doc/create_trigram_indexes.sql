BEGIN;

CREATE index buildings_name_trgm_idx ON osm_buildings USING GIN (name gin_trgm_ops);
CREATE index buildings_road_trgm_idx ON osm_buildings USING GIN (road gin_trgm_ops);
CREATE index buildings_house_number_idx ON osm_buildings using BTREE (house_number);
CREATE index admin_name_trgm_idx ON osm_admin USING GIN (name gin_trgm_ops);
CREATE index roads_road_trgm_idx ON osm_roads USING GIN (road gin_trgm_ops);
CREATE index postcode_idx ON osm_postal_code using BTREE (postcode);

COMMIT;
