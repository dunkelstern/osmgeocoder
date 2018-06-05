BEGIN;

CREATE index name_trgm_idx ON osm_buildings USING GIST (name gist_trgm_ops);
CREATE index road_trgm_idx ON osm_buildings USING GIST (road gist_trgm_ops);
CREATE index city_trgm_idx ON osm_buildings USING GIST (city gist_trgm_ops);
CREATE index postcode_trgm_idx ON osm_buildings USING GIST (postcode gist_trgm_ops);
CREATE index house_number_idx ON osm_buildings using BTREE (house_number);

COMMIT;
