DO
$$
DECLARE
	r record;
	oa_exists boolean;
BEGIN
    SELECT EXISTS (
        SELECT 1
        FROM   information_schema.tables 
        WHERE  table_schema = 'public'
        AND    table_name = 'oa_city'
    ) INTO oa_exists;

	IF oa_exists THEN 
		FOR r IN
			SELECT x.id, a."name" AS city
			FROM (
				SELECT
					c.id,
					c.city,
					gis.ST_Centroid(gis.ST_Collect(array_agg(h.location))) AS centroid
				FROM public.oa_city c
				JOIN public.oa_street s ON c.id = s.city_id
				JOIN public.oa_house h ON s.id = h.street_id
				WHERE c.city = ''
				GROUP BY c.id
			) x
			JOIN public.osm_admin a ON (a.admin_level = 8 AND gis.ST_Contains(a.geometry, x.centroid))
		LOOP
			UPDATE public.oa_city SET city = r.city WHERE id = r.id;
		END LOOP;
	END IF;
END;
$$ LANGUAGE 'plpgsql';
