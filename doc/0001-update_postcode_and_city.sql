BEGIN;

--
-- buildings
--

WITH updated AS (
    -- select all buildings which have no postal code set
    SELECT
        np.id, pc.postcode, am.name AS city
    FROM (SELECT * FROM osm_buildings WHERE postcode = '') np
    -- join postal code table to update the code
    LEFT JOIN osm_postal_code pc
        ON ST_Contains(pc.geometry, ST_Centroid(np.geometry))
    -- join admin border table to update city names
    LEFT JOIN osm_admin am
        ON (am.admin_level = 8 AND ST_Contains(am.geometry, ST_Centroid(np.geometry)))
)

-- update all buildings with corrected values from above
UPDATE osm_buildings b
SET
    postcode = updated.postcode,
    city = updated.city
FROM updated
WHERE b.id = updated.id;


--
-- roads
--

WITH updated AS (
    -- select all roads which have no postal code set
    -- (probably all, but we try to not destroy any data here)
    SELECT
        np.id, pc.postcode, am.name AS city
    FROM (SELECT * FROM osm_roads WHERE postcode = '') np
    -- join postal code table to update the code
    LEFT JOIN osm_postal_code pc
        ON ST_Contains(pc.geometry, ST_Centroid(np.geometry))
    -- join admin border table to update city names
    LEFT JOIN osm_admin am
        ON (am.admin_level = 8 AND ST_Contains(am.geometry, ST_Centroid(np.geometry)))
)

-- update all roads with corrected values from above
UPDATE osm_roads r
SET
    postcode = updated.postcode,
    city = updated.city
FROM updated
WHERE r.id = updated.id;

COMMIT;
