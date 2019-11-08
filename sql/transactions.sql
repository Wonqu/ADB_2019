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


-- use this to execute/see results

BEGIN TRANSACTION;
SELECT
       timed_execution_4() as time_ms,
       'transaction_4' as tr_name
;
ROLLBACK;
