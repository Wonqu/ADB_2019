
-- trash_buffers may once in a while grow big and queries on it take lots of time, so it's feasible to delete it from time to time
DROP TABLE trash_buffers;


-- Necessary to create this table so blocks below can use it to flush buffers
CREATE UNLOGGED TABLE trash_buffers (a char(5000));
ALTER TABLE trash_buffers ALTER COLUMN a SET STORAGE PLAIN;


-- Execute blocks below to trash buffers, may be necessary to do it more than once
DO LANGUAGE plpgsql $$ BEGIN
    FOR i IN 1..(SELECT setting::bigint
                 FROM pg_settings
                 WHERE name = 'shared_buffers') + 10000 LOOP
        INSERT INTO trash_buffers VALUES ('x');
    END LOOP;
END; $$ ;

DO LANGUAGE plpgsql
$$
    DECLARE
        row record;
    BEGIN
       FOR row IN SELECT * FROM trash_buffers
       LOOP
       END LOOP;
    END
$$;
--


-- Necessary to do only once, to enable postgres extension that allows reading buffers
CREATE EXTENSION pg_buffercache;


-- Read buffer data and see if any non pg_* tables are in it
SELECT c.relname,
      count(*) AS buffers,
      pg_size_pretty(count(*) * 8192) AS size,
      pg_size_pretty(pg_relation_size(c.oid)) "rel size",
      pg_size_pretty(sum(count(*)) over () * 8192) "cache size",
      to_char(count(*) / (sum(count(*)) over ()) * 100, '990.99%') as "cache %",
      to_char(count(*)::double precision / (SELECT setting::bigint
                                            FROM pg_settings
                                            WHERE name = 'shared_buffers') * 100,
              '990.99%') AS "shared buffers %",
      to_char(CASE pg_relation_size(c.oid)
              WHEN 0 THEN 100
              ELSE (count(*) * 8192 * 100) / pg_relation_size(c.oid)::float
              END, '990.99%') AS "rel %"
FROM pg_class c
INNER JOIN pg_buffercache b ON b.relfilenode=c.relfilenode
INNER JOIN pg_database d ON (b.reldatabase=d.oid AND d.datname=current_database())
GROUP BY c.relname, c.oid
ORDER BY 2 DESC LIMIT 10;