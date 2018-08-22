--
-- Run this function after updating the OSM data
-- Runtime for "Germany" on SSD with Intel(R) Core(TM) i7-7700K CPU @ 4.20GHz: 24 Seconds
--
CREATE OR REPLACE FUNCTION build_wordlist() RETURNS void AS
$$
DECLARE
    w text[];
BEGIN
    -- clean state
    DROP TABLE IF EXISTS osm_wordlist_temp;
    DROP TABLE IF EXISTS osm_wordlist;

    -- temporary collection table
    CREATE TEMPORARY TABLE osm_wordlist_temp (
        word TEXT
    );

    -- final wordlist table
    CREATE TABLE osm_wordlist (
        word TEXT PRIMARY KEY,
        ct INT DEFAULT 1
    );

    -- create word list
    FOR w IN
        SELECT regexp_split_to_array(road, '\W') AS word FROM osm_roads
    LOOP
        INSERT INTO osm_wordlist_temp (word) VALUES (unnest(w));
    END LOOP;

    -- create index
    CREATE INDEX wordlist_word_idx ON osm_wordlist_temp USING BTREE(word);

    -- reduce table by grouping by word (using the index just created)
    INSERT INTO osm_wordlist (word, ct) SELECT word, count(*) FROM osm_wordlist_temp GROUP BY word;

    -- drop temporary table
    DROP TABLE osm_wordlist_temp;

    -- create new index on words
    CREATE INDEX wordlist_word_idx ON osm_wordlist USING BTREE(word);

    -- create trigram index, just in case you want to search with a trigram search
    CREATE INDEX wordlist_word_trgm_idx ON osm_wordlist USING GIN(word gin_trgm_ops);

    -- create metaphone trigram index, sounds complicated but really isn't
    -- this index allows to search with the '%' operator in the metaphone index
    -- so we can find words that sound the same or just a bit different than the one the
    -- user searches for. This also allows to find incomplete words (to be used while typing)
    CREATE INDEX wordlist_word_dmetaphone_idx ON osm_wordlist USING GIN(dmetaphone(word) gin_trgm_ops);
    CREATE INDEX wordlist_word_dmetaphone_alt_idx ON osm_wordlist USING GIN(dmetaphone_alt(word) gin_trgm_ops);

    -- tell postgres to update the query planner for the newly created indices
    ANALYZE osm_wordlist;
END
$$ LANGUAGE 'plpgsql';

-- build the list for the first time
SELECT build_wordlist();

