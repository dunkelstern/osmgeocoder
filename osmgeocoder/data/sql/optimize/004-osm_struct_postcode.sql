DROP TABLE IF EXISTS public.osm_struct_postcode;

SELECT 
    crypto.gen_random_uuid() AS id,
    nullif(coalesce(a8.name, a6.name), '') as name,
    pc.postcode,
    pc.geometry as area,
    gis.ST_Centroid(pc.geometry) as center
INTO public.osm_struct_postcode
FROM public.osm_postal_code pc
LEFT JOIN public.osm_admin a8 ON 
    a8.admin_level = 8
    AND gis.ST_Intersects(pc.geometry, a8.geometry)
LEFT JOIN public.osm_admin a6 ON 
    a6.admin_level = 6
    AND gis.ST_Intersects(pc.geometry, a6.geometry)
GROUP BY 
    coalesce(a8.name, a6.name),
    pc.postcode,
    pc.geometry;

CREATE INDEX osm_struct_postcode_name_idx ON public.osm_struct_postcode USING BTREE(name);
CREATE INDEX osm_struct_postcode_postcode_idx ON public.osm_struct_postcode USING BTREE(postcode);

CREATE INDEX osm_struct_postcode_area_idx ON public.osm_struct_postcode USING GIST(area);
CREATE INDEX osm_struct_postcode_center_idx ON public.osm_struct_postcode USING GIST(center);

CREATE INDEX osm_struct_postcode_name_trgm_idx ON public.osm_struct_postcode USING GIN(name gin_trgm_ops);
CREATE INDEX osm_struct_postcode_name_dmetaphone_idx ON public.osm_struct_postcode USING GIN(str.dmetaphone(name) gin_trgm_ops);
CREATE INDEX osm_struct_postcode_name_dmetaphone_alt_idx ON public.osm_struct_postcode USING GIN(str.dmetaphone_alt(name) gin_trgm_ops);

ANALYZE public.osm_struct_postcode;