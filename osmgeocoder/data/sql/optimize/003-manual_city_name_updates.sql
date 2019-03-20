UPDATE public.oa_city SET city = 'Bucharest' WHERE license_id = (SELECT id FROM public.oa_license WHERE source = 'ro/bucharest' LIMIT 1);
UPDATE public.oa_city SET city = 'Wien' WHERE license_id = (SELECT id FROM public.oa_license WHERE source = 'at/city_of_vienna' LIMIT 1);
UPDATE public.oa_city SET city = 'Köln' WHERE license_id = (SELECT id FROM public.oa_license WHERE source = 'de/nw/city_of_cologne' LIMIT 1);
