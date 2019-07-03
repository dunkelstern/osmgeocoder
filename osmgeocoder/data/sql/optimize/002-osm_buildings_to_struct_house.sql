CREATE INDEX IF NOT EXISTS osm_buildings_empty_house_number_idx ON public.osm_buildings((house_number <> '')) WHERE house_number <> '';
ANALYZE osm_buildings;

INSERT INTO public.osm_struct_house (id, osm_id, city, postcode, street, house_number, geometry)
SELECT 
	crypto.gen_random_uuid() AS id,
	b.osm_id,
	'' AS city,
	p.postcode,
	b.street,
	b.house_number,
	gis.ST_Centroid(b.geometry) AS geometry
FROM public.osm_buildings b 
JOIN public.osm_postal_code p ON gis.ST_Within(gis.ST_Centroid(b.geometry), p.geometry)
WHERE b.house_number <> '';