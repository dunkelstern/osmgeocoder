--
-- Run this function after updating the OSM data
-- Runtime for "Europe" on SSD with Intel(R) Core(TM) i7-7700K CPU @ 4.20GHz: ca. 600 Seconds
--
CREATE OR REPLACE FUNCTION public.build_wordlist() RETURNS void AS
$$
DECLARE
    w text[];
    oa_exists boolean;
    osm_exists boolean;
BEGIN
    SELECT EXISTS (
        SELECT 1
        FROM   information_schema.tables 
        WHERE  table_schema = 'public'
        AND    table_name = 'oa_city'
    ) INTO oa_exists;

    SELECT EXISTS (
        SELECT 1
        FROM   information_schema.tables 
        WHERE  table_schema = 'public'
        AND    table_name = 'osm_struct_cities'
    ) INTO osm_exists;

    -- clean state
    DROP TABLE IF EXISTS public.wordlist_temp;
    DROP TABLE IF EXISTS public.wordlist;

    -- temporary collection table
    CREATE TEMPORARY TABLE wordlist_temp (
        word TEXT,
        ct INT
    );

    -- final wordlist table
    CREATE TABLE public.wordlist (
        word TEXT PRIMARY KEY,
        ct INT DEFAULT 1
    );

    -- create word list
    IF osm_exists THEN
        INSERT INTO wordlist_temp (word, ct) SELECT word, ct FROM (
            SELECT unnest(regexp_split_to_array(c.name, '\W')) as word, count(s.*) as ct
                FROM public.osm_struct_cities c
                JOIN public.osm_struct_streets s ON c.id = s.city_id
            GROUP BY c.name
            UNION ALL
            SELECT unnest(regexp_split_to_array(s.name, '\W')) as word, count(h.*) as ct
                FROM public.osm_struct_streets s
                JOIN public.osm_struct_house h ON s.id = h.street_id
            GROUP BY s.name
        ) x;
    END IF;

    IF oa_exists THEN 
        INSERT INTO wordlist_temp (word, ct) SELECT word, ct FROM (
            SELECT unnest(regexp_split_to_array(c.city, '\W')) as word, count(s.*) as ct
                FROM public.oa_city c
                JOIN public.oa_street s ON c.id = s.city_id
            GROUP BY c.city
            UNION ALL
            SELECT unnest(regexp_split_to_array(s.street, '\W')) as word, count(h.*) as ct
                FROM public.oa_street s
                JOIN public.oa_house h ON s.id = h.street_id
            GROUP BY s.street
        ) x;
    END IF;

    -- create index
    CREATE INDEX wordlist_word_idx ON wordlist_temp USING BTREE(word);

    -- reduce table by grouping by word (using the index just created)
    INSERT INTO public.wordlist (word, ct) SELECT word, sum(ct) FROM wordlist_temp GROUP BY word;

    -- drop temporary table
    DROP TABLE wordlist_temp;

    -- create new index on words
    CREATE INDEX wordlist_word_idx ON public.wordlist USING BTREE(word);

    -- create trigram index, just in case you want to search with a trigram search
    CREATE INDEX wordlist_word_trgm_idx ON public.wordlist USING GIN(word gin_trgm_ops);

    -- create metaphone trigram index, sounds complicated but really isn't
    -- this index allows to search with the '%' operator in the metaphone index
    -- so we can find words that sound the same or just a bit different than the one the
    -- user searches for. This also allows to find incomplete words (to be used while typing)
    CREATE INDEX wordlist_word_dmetaphone_idx ON public.wordlist USING GIN(str.dmetaphone(word) gin_trgm_ops);
    CREATE INDEX wordlist_word_dmetaphone_alt_idx ON public.wordlist USING GIN(str.dmetaphone_alt(word) gin_trgm_ops);

    -- tell postgres to update the query planner for the newly created indices
    ANALYZE public.wordlist;
END
$$ LANGUAGE 'plpgsql';

-- build the list for the first time
SELECT public.build_wordlist();
