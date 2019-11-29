-- TRANSACTION 1
CREATE OR REPLACE FUNCTION transaction_1(
    dscr text,
    min_price money,
    op_time timestamp,
    cl_time timestamp,
    nb_of_pics int
)
    RETURNS void
AS
$$
DECLARE
    user_id int := (
        SELECT id FROM users
        ORDER BY random()
        LIMIT 1
    );
BEGIN
    WITH
         new_listing AS (
            INSERT INTO listings (seller_id, description, minimal_price, opening_time, closing_time, status)
                VALUES (user_id, dscr, min_price, op_time, cl_time, 'open')
            RETURNING seller_id AS id
        ),
        new_pictures AS (
            INSERT INTO pictures
                SELECT generate_series(100001, nb_of_pics) AS id,
                       random()::text                 AS description,
                       md5(random()::text)            AS storage_key,
                       random()::int                  AS width,
                       random()::int                  AS height,
                       clock_timestamp()              AS upload_time
            RETURNING id
        )
    INSERT INTO listings_pictures (listing_id, picture_id)
    SELECT DISTINCT new_listing.id, new_pictures.id
    FROM new_listing
    JOIN new_pictures ON true;
end ;
$$
    language plpgsql;
-- END TRANSACTION 1
-- TRANSACTION 2

create or replace function transaction_2(p_uprise numeric)
    returns void
AS
$$
declare
begin
--     create table bid_ids as
--     select id from bids
--     where bid_status = 'active';

    with bid_ids as (
        select id from bids
        where bid_status = 'active'
        LIMIT 5
    )
    insert into bids(listing_id, bidder_id, bid_price, bid_time, bid_status)
    select listing_id, bidder_id, (1 + p_uprise) * bid_price, now() , 'active' from bids
    where id in (select id from bid_ids);

    with bid_ids as (
            select id from bids
        where bid_status = 'active'
    )
    update bids
    set bid_status = 'inactive'
    where id in (select id from bid_ids);
end ;
$$
    language plpgsql;

-- END TRANSACTION 2

-- TRANSACTION 3
CREATE OR REPLACE FUNCTION transaction_3() RETURNS void AS
$$
    BEGIN
        WITH
            USERS_TO_EXPIRE as (
                SELECT id
                FROM users
                WHERE users.active = FALSE
                LIMIT 200
            ),
            LISTING_ID_TO_EXPIRE as (
                UPDATE listings
                SET status = 'expired'
                WHERE seller_id IN (SELECT id FROM USERS_TO_EXPIRE)
                RETURNING listings.id as id
            ),
            PICTURES_IDS as (
                SELECT listings_pictures.picture_id as id
                FROM listings_pictures
                WHERE listings_pictures.listing_id IN (SELECT id from LISTING_ID_TO_EXPIRE)
            ),
            PICTURES_TO_DELETE_IDS AS (
                DELETE
                FROM listings_pictures
                WHERE listings_pictures.picture_id IN (select id from PICTURES_IDS)
                RETURNING picture_id as id
            ),
            BIDS_TO_EXPIRE AS (
                UPDATE bids
                SET bid_status = 'inactive'
                WHERE bids.id IN (SELECT ID FROM LISTING_ID_TO_EXPIRE)
            )
        DELETE
        FROM pictures
        WHERE pictures.id IN (SELECT id from PICTURES_TO_DELETE_IDS);
    END;
$$
LANGUAGE PLPGSQL;
-- END TRANSACTION 3

-- TRANSACTION 4
-- DROP FUNCTION transaction_4;
CREATE OR REPLACE FUNCTION transaction_4(u_id INTEGER)
RETURNS TABLE(
    user_id integer,
    bid_year double precision,
    bid_month double precision,
    opened_count numeric,
    participated_count bigint,
    bids_count bigint,
    mean interval,
    average_bid numeric,
    avg_outbid text,
    avg_money money
) AS
$$
    BEGIN
        RETURN QUERY WITH
            LISTINGS_OPENED AS (
                SELECT
                    u_id AS user_id,
                    COUNT(listings.id) AS opened_count,
                    extract(YEAR FROM bids.bid_time) AS bid_year,
                    extract(MONTH FROM bids.bid_time) AS bid_month
                FROM listings
                JOIN bids ON listings.id = bids.listing_id
                WHERE seller_id = u_id
                GROUP BY seller_id, bid_year, bid_month
            ),
            LISTINGS_PARTICIPATED AS (
                SELECT
                    u_id AS user_id,
                    COUNT(listing_id) AS participated_count,
                    extract(YEAR FROM bids.bid_time) AS bid_year,
                    extract(MONTH FROM bids.bid_time) AS bid_month
                FROM users
                JOIN bids ON bids.bidder_id = users.id
                WHERE users.id = u_id
                GROUP BY users.id, bids.bidder_id, bids.bid_time
            ),
            NUMBER_OF_BIDS AS (
                SELECT
                    bids.bidder_id AS user_id,
                    COUNT(bids.id) AS bids_count,
                    extract(YEAR FROM bids.bid_time) AS bid_year,
                    extract(MONTH FROM bids.bid_time) AS bid_month
                FROM bids
                WHERE bids.bidder_id = u_id
                GROUP BY bids.bidder_id, bids.bid_time
            ),
            MEAN_TIME_BETWEEN_BIDS AS (
                SELECT
                    u_id AS user_id,
                    AVG(bid_interval) as mean,
                    bids_with_interval.bid_year AS bid_year,
                    bids_with_interval.bid_month AS bid_month
                FROM (
                    SELECT
                        bids.bidder_id,
                        bids.bid_time - lag(bids.bid_time) OVER (ORDER BY bids.bid_time) AS bid_interval,
                        extract(YEAR FROM bids.bid_time) AS bid_year,
                        extract(MONTH FROM bids.bid_time) AS bid_month
                    FROM bids
                    GROUP BY bids.bidder_id, bid_year, bid_month, bid_time
                ) AS bids_with_interval
                WHERE bids_with_interval.bidder_id = u_id
                GROUP BY bids_with_interval.bidder_id, bids_with_interval.bid_year, bids_with_interval.bid_month
            ),
            AVG_BID_PRICE AS (
                SELECT
                    u_id AS user_id,
                    AVG(bids.bid_price::numeric) AS average_bid,
                    extract(YEAR FROM bids.bid_time) AS bid_year,
                    extract(MONTH FROM bids.bid_time) AS bid_month
                FROM bids
                WHERE bids.bidder_id = u_id
                GROUP BY bids.bid_time, bids.bidder_id
            ),
            AVG_MINMAL_PRICE AS (
                SELECT
                    u_id AS user_id,
                    CONCAT(
                        ROUND(
                                AVG(((bids.bid_price::numeric - l.minimal_price::numeric) / l.minimal_price::numeric) / 100), 2
                            )::varchar, '%') AS avg_outbid,
                    extract(YEAR FROM bids.bid_time) AS bid_year,
                    extract(MONTH FROM bids.bid_time) AS bid_month
                FROM bids
                JOIN listings l on bids.listing_id = l.id
                WHERE bids.bidder_id = u_id
                GROUP BY bids.bid_time, bids.listing_id, bids.bidder_id
            ),
            AVG_MONEY_SPENT AS (
                SELECT
                    u_id AS user_id,
                    AVG(sales.sale_price::numeric + sales.marketplace_brokerage::numeric)::money AS avg_money,
                    extract(YEAR FROM sales.payment_time) AS bid_year,
                    extract(MONTH FROM sales.payment_time) AS bid_month
                FROM sales
                WHERE sales.listing_id IN (
                    SELECT
                        listings.id
                    FROM listings
                    JOIN bids ON listings.id = bids.listing_id
                    WHERE bids.bid_status = 'winner' AND bids.bidder_id = u_id
                    GROUP BY listings.id
                )
                GROUP BY sales.payment_time
            )
        SELECT
            users.id AS id,
            LISTINGS_OPENED.bid_year AS bid_year,
            LISTINGS_OPENED.bid_month AS bid_month,
            SUM(LISTINGS_OPENED.opened_count) as opened_count,
            COUNT(LISTINGS_PARTICIPATED.participated_count) AS participated_count,
            COUNT(NUMBER_OF_BIDS.bids_count) AS bids_count,
            MEAN_TIME_BETWEEN_BIDS.mean AS mean,
            AVG_BID_PRICE.average_bid AS average_bid,
            AVG_MINMAL_PRICE.avg_outbid AS avg_outbid,
            AVG_MONEY_SPENT.avg_money AS avg_money
        FROM users
        LEFT JOIN LISTINGS_OPENED on LISTINGS_OPENED.user_id = users.id
        LEFT JOIN LISTINGS_PARTICIPATED on LISTINGS_PARTICIPATED.bid_year = LISTINGS_OPENED.bid_year AND LISTINGS_PARTICIPATED.bid_month = LISTINGS_OPENED.bid_month
        LEFT JOIN NUMBER_OF_BIDS on NUMBER_OF_BIDS.bid_year = LISTINGS_OPENED.bid_year AND NUMBER_OF_BIDS.bid_month = LISTINGS_OPENED.bid_month
        LEFT JOIN MEAN_TIME_BETWEEN_BIDS on MEAN_TIME_BETWEEN_BIDS.bid_year = LISTINGS_OPENED.bid_year AND MEAN_TIME_BETWEEN_BIDS.bid_month = LISTINGS_OPENED.bid_month
        LEFT JOIN AVG_BID_PRICE on AVG_BID_PRICE.bid_year = LISTINGS_OPENED.bid_year AND AVG_BID_PRICE.bid_month = LISTINGS_OPENED.bid_month
        LEFT JOIN AVG_MINMAL_PRICE on AVG_MINMAL_PRICE.bid_year = LISTINGS_OPENED.bid_year AND AVG_MINMAL_PRICE.bid_month = LISTINGS_OPENED.bid_month
        LEFT JOIN AVG_MONEY_SPENT on AVG_MONEY_SPENT.bid_year = LISTINGS_OPENED.bid_year AND AVG_MONEY_SPENT.bid_month = LISTINGS_OPENED.bid_month
        WHERE users.id = u_id
        GROUP BY
                users.id,
                LISTINGS_OPENED.bid_year,
                LISTINGS_OPENED.bid_month,
                MEAN_TIME_BETWEEN_BIDS.mean,
                AVG_BID_PRICE.average_bid,
                AVG_MINMAL_PRICE.avg_outbid,
                AVG_MONEY_SPENT.avg_money
        ;
    END;
$$
LANGUAGE PLPGSQL;
-- END TRANSACTION 4

-- TRANSACTION 5
create or replace function transaction_5(p_time_from timestamp,
                                         p_time_to timestamp)
    returns table
            (
                listing_id                                 int,
                seller_id                                  int,
                minimal_price                              money,
                highest_bid                                money,
                average_bid                                money,
                number_of_bids                             int,
                latest_bid_before_closing                  timestamp,
                latest_bid_before_closing_time_remaining   interval,
                number_of_pictures_to_number_of_bids_ratio float,
                number_of_bidders                          int

            )
AS
$$
declare
begin
    return query
        select b.listing_id,
               l.seller_id,
               l.minimal_price,
               max(bid_price),
               avg(bid_price::numeric)::money,
               count(b.*)::int,
               max(bid_time),
               min(l.closing_time - b.bid_time),
               (count(lp.picture_id) / count(b.*))::float,
               count(b.bidder_id)::int
        from bids b
                 join listings l on b.listing_id = l.id
                 join listings_pictures lp on l.id = lp.listing_id
        where b.bid_time between p_time_from and p_time_to
        group by 1, 2, 3;
end ;
$$
    language plpgsql;

-- END TRANSACTION 5

-- Timed Execution wrappers

CREATE OR REPLACE FUNCTION timed_execution_1() RETURNS integer AS
$$
    DECLARE
        curtime timestamp := clock_timestamp();
        endtime timestamp;
    BEGIN
        PERFORM transaction_1(
            'random description'::text,
            '$20.30'::money,
            clock_timestamp()::timestamp,
            (clock_timestamp()::timestamp + '30 days'::interval)::timestamp,
            200000::int
            );
        endtime := clock_timestamp();
        return 1000 * (extract (epoch from endtime)::numeric - extract(epoch from curtime)::numeric);
    END;
$$
LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION timed_execution_2() RETURNS integer AS
$$
    DECLARE
        curtime timestamp := clock_timestamp();
        endtime timestamp;
    BEGIN
        PERFORM transaction_2(10);
        endtime := clock_timestamp();
        return 1000 * (extract (epoch from endtime)::numeric - extract(epoch from curtime)::numeric);
    END
$$
LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION timed_execution_3() RETURNS integer AS
$$
    DECLARE
        curtime timestamp := clock_timestamp();
        endtime timestamp;
    BEGIN
        PERFORM transaction_3();
        endtime := clock_timestamp();
        return 1000 * (extract (epoch from endtime)::numeric - extract(epoch from curtime)::numeric);
    END;
$$
LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION timed_execution_4() RETURNS integer AS
$$
    DECLARE
        curtime timestamp := clock_timestamp();
        endtime timestamp;
    BEGIN
        PERFORM transaction_4(100);
        endtime := clock_timestamp();
        return 1000 * (extract (epoch from endtime)::numeric - extract(epoch from curtime)::numeric);
    END;
$$
LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION timed_execution_5() RETURNS integer AS
$$
    DECLARE
        curtime timestamp := clock_timestamp();
        endtime timestamp;
    BEGIN
        PERFORM transaction_5((now() - interval '1 year')::timestamp, now()::timestamp);
        endtime := clock_timestamp();
        return 1000 * (extract (epoch from endtime)::numeric - extract(epoch from curtime)::numeric);
    END;
$$
LANGUAGE PLPGSQL;

-- END Timed Execution wrappers


-- MEASURE

-- this is used to store execution time in measure table using autonomous transaction
-- take note that it is necessary to create such table (preferably in another database and schema, but can be in the same)
-- create table measures
-- (
--     id               serial not null,
--     measure_time_ms  numeric(1000),
--     transaction_name varchar,
--     improvement_name text
-- );

CREATE OR REPLACE FUNCTION log_dblink(v NUMERIC, t TEXT, i TEXT)
 RETURNS void
 LANGUAGE sql
AS $function$
    -- change dbname to postgres and measures.measures to public.measures if measures table is in the same schema
   select dblink('host=/var/run/postgresql port=5432 user=postgres dbname=measures',
    format('INSERT INTO measures.measures (measure_time_ms, transaction_name, improvement_name) VALUES (%L, %L, %L)', v, t, i)
   )
$function$;

CREATE OR REPLACE PROCEDURE destroy_buffers()
LANGUAGE plpgsql AS
$$
    DECLARE
        row record;
    BEGIN
        FOR i IN 1..(
            SELECT setting::bigint
            FROM pg_settings
            WHERE name = 'shared_buffers') + 10000
        LOOP
            INSERT INTO trash_buffers VALUES ('x');
        END LOOP;
        FOR row IN SELECT * FROM trash_buffers
        LOOP
        END LOOP;
    END;
$$;

CREATE OR REPLACE PROCEDURE measure_1(imp_name TEXT)
LANGUAGE plpgsql AS
$$
    BEGIN
        PERFORM log_dblink(timed_execution_1(), 'transaction_1'::TEXT, imp_name);
    END;
$$;

CREATE OR REPLACE PROCEDURE measure_2(imp_name TEXT)
LANGUAGE plpgsql AS
$$
    BEGIN
        PERFORM log_dblink(timed_execution_2(), 'transaction_2'::TEXT, imp_name);
    END;
$$;

CREATE OR REPLACE PROCEDURE measure_3(imp_name TEXT)
LANGUAGE plpgsql AS
$$
    BEGIN
        PERFORM log_dblink(timed_execution_3(), 'transaction_3'::TEXT, imp_name);
    END;
$$;

CREATE OR REPLACE PROCEDURE measure_4(imp_name TEXT)
LANGUAGE plpgsql AS
$$
    BEGIN
        PERFORM log_dblink(timed_execution_4(), 'transaction_4'::TEXT, imp_name);
    END;
$$;

CREATE OR REPLACE PROCEDURE measure_5(imp_name TEXT)
LANGUAGE plpgsql AS
$$
    BEGIN
        PERFORM log_dblink(timed_execution_5(), 'transaction_5'::TEXT, imp_name);
    END;
$$;


BEGIN TRANSACTION;
    SAVEPOINT a; CALL measure_1('no_improvement'::TEXT); ROLLBACK TO SAVEPOINT a; CALL destroy_buffers();
    SAVEPOINT a; CALL measure_2('no_improvement'::TEXT); ROLLBACK TO SAVEPOINT a; CALL destroy_buffers();
    SAVEPOINT a; CALL measure_3('no_improvement'::TEXT); ROLLBACK TO SAVEPOINT a; CALL destroy_buffers();
    SAVEPOINT a; CALL measure_4('no_improvement'::TEXT); ROLLBACK TO SAVEPOINT a; CALL destroy_buffers();
    SAVEPOINT a; CALL measure_5('no_improvement'::TEXT); ROLLBACK TO SAVEPOINT a; CALL destroy_buffers();
ROLLBACK;

BEGIN TRANSACTION;
    -- IMPROVEMENT 1
    CREATE INDEX idx_btree_bids_listing_id ON bids USING btree (listing_id);
    CREATE INDEX idx_btree_bids_bidder_id ON bids USING btree (bidder_id);
    CREATE INDEX idx_btree_listings_seller_id ON listings USING btree (seller_id);
    CREATE INDEX idx_btree_listings_pictures_listing_id ON listings_pictures USING btree (listing_id);
    CREATE INDEX idx_btree_listings_pictures_picture_id ON listings_pictures USING btree (picture_id);

    SAVEPOINT a; CALL measure_1('improvement_1'::TEXT); ROLLBACK TO SAVEPOINT a; CALL destroy_buffers();
    SAVEPOINT a; CALL measure_2('improvement_1'::TEXT); ROLLBACK TO SAVEPOINT a; CALL destroy_buffers();
    SAVEPOINT a; CALL measure_3('improvement_1'::TEXT); ROLLBACK TO SAVEPOINT a; CALL destroy_buffers();
    SAVEPOINT a; CALL measure_4('improvement_1'::TEXT); ROLLBACK TO SAVEPOINT a; CALL destroy_buffers();
    SAVEPOINT a; CALL measure_5('improvement_1'::TEXT); ROLLBACK TO SAVEPOINT a; CALL destroy_buffers();
ROLLBACK;

BEGIN TRANSACTION;
    -- IMPROVEMENT 2
    ALTER TABLE bids ALTER COLUMN bid_status TYPE VARCHAR using bid_status::TEXT;
    CREATE INDEX idx_brin_bids_bid_status ON bids USING brin(bid_status);

    SAVEPOINT a; CALL measure_1('improvement_2'::TEXT); ROLLBACK TO SAVEPOINT a; CALL destroy_buffers();
    SAVEPOINT a; CALL measure_2('improvement_2'::TEXT); ROLLBACK TO SAVEPOINT a; CALL destroy_buffers();
    SAVEPOINT a; CALL measure_3('improvement_2'::TEXT); ROLLBACK TO SAVEPOINT a; CALL destroy_buffers();
    SAVEPOINT a; CALL measure_4('improvement_2'::TEXT); ROLLBACK TO SAVEPOINT a; CALL destroy_buffers();
    SAVEPOINT a; CALL measure_5('improvement_2'::TEXT); ROLLBACK TO SAVEPOINT a; CALL destroy_buffers();
ROLLBACK;

BEGIN TRANSACTION;
    -- IMPROVEMENT 3
    -- rename current table to backup
    ALTER TABLE bids RENAME TO bids_backup;

    -- Create partitioned table
    create table bids
    (
        id serial not null,
        listing_id integer
            constraint bids_listing_id_fkey
                references listings,
        bidder_id  integer
            constraint bids_bidder_id_fkey
                references users,
        bid_price  money,
        bid_time   timestamp,
        bid_status "BID_STATUS",
        PRIMARY KEY(id, bid_status)
    )
    PARTITION BY LIST(bid_status);
    CREATE TABLE bids_active PARTITION OF bids FOR VALUES IN ('active');
    CREATE TABLE bids_inactive PARTITION OF bids FOR VALUES IN ('inactive');
    CREATE TABLE bids_winner PARTITION OF bids FOR VALUES IN ('winner');
    CREATE TABLE bids_loser PARTITION OF bids FOR VALUES IN ('loser');
    CREATE TABLE bids_default PARTITION OF bids DEFAULT;

    -- copy data from old table
    INSERT INTO bids (SELECT * FROM bids_backup);

    SAVEPOINT a; CALL measure_1('improvement_3'::TEXT); ROLLBACK TO SAVEPOINT a; CALL destroy_buffers();
    -- measure 2 for improvement 3 is the one that breaks
    SAVEPOINT a; CALL measure_2('improvement_3'::TEXT); ROLLBACK TO SAVEPOINT a; CALL destroy_buffers();
    --
    SAVEPOINT a; CALL measure_3('improvement_3'::TEXT); ROLLBACK TO SAVEPOINT a; CALL destroy_buffers();
    SAVEPOINT a; CALL measure_4('improvement_3'::TEXT); ROLLBACK TO SAVEPOINT a; CALL destroy_buffers();
    SAVEPOINT a; CALL measure_5('improvement_3'::TEXT); ROLLBACK TO SAVEPOINT a; CALL destroy_buffers();
ROLLBACK;

BEGIN TRANSACTION;
    -- IMPROVEMENT 4
    -- rename current table to backup
    ALTER TABLE bids RENAME TO bids_backup;

    -- Create partitioned table
    create table bids
    (
        id serial not null,
        listing_id integer
            constraint bids_listing_id_fkey
                references listings,
        bidder_id  integer
            constraint bids_bidder_id_fkey
                references users,
        bid_price  money,
        bid_time   timestamp,
        bid_status "BID_STATUS",
        PRIMARY KEY(id, bid_time)
    )
    PARTITION BY RANGE(bid_time);

    create table bids_p_2018_11
    partition of bids
    for values from ('2018-11-01') to ('2018-12-01');

    create table bids_p_2018_12
    partition of bids
    for values from ('2018-12-01') to ('2019-1-01');

    create table bids_p_2019_1
    partition of bids
    for values from ('2019-1-01') to ('2019-2-01');

    create table bids_p_2019_2
    partition of bids
    for values from ('2019-2-01') to ('2019-3-01');

    create table bids_p_2019_3
    partition of bids
    for values from ('2019-3-01') to ('2019-4-01');

    create table bids_p_2019_4
    partition of bids
    for values from ('2019-4-01') to ('2019-5-01');

    create table bids_p_2019_5
    partition of bids
    for values from ('2019-5-01') to ('2019-6-01');

    create table bids_p_2019_6
    partition of bids
    for values from ('2019-6-01') to ('2019-7-01');

    create table bids_p_2019_7
    partition of bids
    for values from ('2019-7-01') to ('2019-8-01');

    create table bids_p_2019_8
    partition of bids
    for values from ('2019-8-01') to ('2019-9-01');

    create table bids_p_2019_9
    partition of bids
    for values from ('2019-9-01') to ('2019-10-01');

    create table bids_p_2019_10
    partition of bids
    for values from ('2019-10-01') to ('2019-11-01');

    create table bids_p_2019_11
    partition of bids
    for values from ('2019-11-01') to ('2019-12-01');

    create table bids_p_default
    partition of bids
    default;

    -- Copy data from old bids to new bids
    INSERT INTO bids (SELECT * FROM bids_backup);

    SAVEPOINT a; CALL measure_1('improvement_4'::TEXT); ROLLBACK TO SAVEPOINT a; CALL destroy_buffers();
    SAVEPOINT a; CALL measure_2('improvement_4'::TEXT); ROLLBACK TO SAVEPOINT a; CALL destroy_buffers();
    SAVEPOINT a; CALL measure_3('improvement_4'::TEXT); ROLLBACK TO SAVEPOINT a; CALL destroy_buffers();
    SAVEPOINT a; CALL measure_4('improvement_4'::TEXT); ROLLBACK TO SAVEPOINT a; CALL destroy_buffers();
    SAVEPOINT a; CALL measure_5('improvement_4'::TEXT); ROLLBACK TO SAVEPOINT a; CALL destroy_buffers();
ROLLBACK;

BEGIN TRANSACTION;
    create index index_bids_bidtime_year on bids using btree(extract(year from bid_time));
    create index index_bids_bidtime_month on bids using btree(extract(month from bid_time));

    SAVEPOINT a; CALL measure_1('improvement_5'::TEXT); ROLLBACK TO SAVEPOINT a; CALL destroy_buffers();
    SAVEPOINT a; CALL measure_2('improvement_5'::TEXT); ROLLBACK TO SAVEPOINT a; CALL destroy_buffers();
    SAVEPOINT a; CALL measure_3('improvement_5'::TEXT); ROLLBACK TO SAVEPOINT a; CALL destroy_buffers();
    SAVEPOINT a; CALL measure_4('improvement_5'::TEXT); ROLLBACK TO SAVEPOINT a; CALL destroy_buffers();
    SAVEPOINT a; CALL measure_5('improvement_5'::TEXT); ROLLBACK TO SAVEPOINT a; CALL destroy_buffers();
ROLLBACK;