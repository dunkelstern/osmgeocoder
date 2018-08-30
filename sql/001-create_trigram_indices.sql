CREATE INDEX IF NOT EXISTS buildings_name_trgm_idx ON osm_buildings USING GIN (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS buildings_road_trgm_idx ON osm_buildings USING GIN (road gin_trgm_ops);
CREATE INDEX IF NOT EXISTS buildings_house_number_idx ON osm_buildings using BTREE (house_number);
CREATE INDEX IF NOT EXISTS admin_name_trgm_idx ON osm_admin USING GIN (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS roads_road_trgm_idx ON osm_roads USING GIN (road gin_trgm_ops);
CREATE INDEX IF NOT EXISTS postcode_trgm_idx ON osm_postal_code using GIN (postcode gin_trgm_ops);
CREATE INDEX IF NOT EXISTS postcode_idx ON osm_postal_code using BTREE (postcode);

ANALYZE osm_buildings;
ANALYZE osm_admin;
ANALYZE osm_roads;
ANALYZE osm_postal_code;
