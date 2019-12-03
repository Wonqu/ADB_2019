-- TRANSACTION 1
CREATE OR REPLACE FUNCTION transaction_1(dscr TEXT,
                                         min_price MONEY,
                                         op_time TIMESTAMP,
                                         cl_time TIMESTAMP,
                                         nb_of_pics INT)
    RETURNS VOID
AS
$$
DECLARE
    user_id INT := (
        SELECT id
        FROM users
        ORDER BY random()
        LIMIT 1
    );
BEGIN
    WITH new_listing AS (
        INSERT INTO listings (seller_id, description, minimal_price, opening_time, closing_time, status)
            VALUES (user_id, dscr, min_price, op_time, cl_time, 'open')
            RETURNING seller_id AS id
    ),
         new_pictures AS (
             INSERT INTO pictures
                 SELECT generate_series(100001, nb_of_pics) AS id,
                        random()::TEXT                      AS description,
                        md5(random()::TEXT)                 AS storage_key,
                        random()::INT                       AS width,
                        random()::INT                       AS height,
                        clock_timestamp()                   AS upload_time
                 RETURNING id
         )
    INSERT
    INTO listings_pictures (listing_id, picture_id)
    SELECT DISTINCT new_listing.id, new_pictures.id
    FROM new_listing
             JOIN new_pictures ON TRUE;
END ;
$$
    LANGUAGE plpgsql;
-- END TRANSACTION 1
-- TRANSACTION 2

CREATE OR REPLACE FUNCTION transaction_2(p_uprise NUMERIC)
    RETURNS VOID
AS
$$
DECLARE
BEGIN
    --     create table bid_ids as
--     select id from bids
--     where bid_status = 'active';

    WITH bid_ids AS (
        SELECT id
        FROM bids
        WHERE bid_status = 'active'
        LIMIT 5
    )
    INSERT
    INTO bids(listing_id, bidder_id, bid_price, bid_time, bid_status)
    SELECT listing_id, bidder_id, (1 + p_uprise) * bid_price, now(), 'active'
    FROM bids
    WHERE id IN (SELECT id FROM bid_ids);

    WITH bid_ids AS (
        SELECT id
        FROM bids
        WHERE bid_status = 'active'
    )
    UPDATE bids
    SET bid_status = 'inactive'
    WHERE id IN (SELECT id FROM bid_ids);
END ;
$$
    LANGUAGE plpgsql;

-- END TRANSACTION 2

-- TRANSACTION 3
CREATE OR REPLACE FUNCTION transaction_3() RETURNS VOID AS
$$
BEGIN
    WITH USERS_TO_EXPIRE AS (
        SELECT id
        FROM users
        WHERE users.active = FALSE
        LIMIT 200
    ),
         LISTING_ID_TO_EXPIRE AS (
             UPDATE listings
                 SET status = 'expired'
                 WHERE seller_id IN (SELECT id FROM USERS_TO_EXPIRE)
                 RETURNING listings.id AS id
         ),
         PICTURES_IDS AS (
             SELECT listings_pictures.picture_id AS id
             FROM listings_pictures
             WHERE listings_pictures.listing_id IN (SELECT id FROM LISTING_ID_TO_EXPIRE)
         ),
         PICTURES_TO_DELETE_IDS AS (
             DELETE
                 FROM listings_pictures
                     WHERE listings_pictures.picture_id IN (SELECT id FROM PICTURES_IDS)
                     RETURNING picture_id AS id
         ),
         BIDS_TO_EXPIRE AS (
             UPDATE bids
                 SET bid_status = 'inactive'
                 WHERE bids.id IN (SELECT ID FROM LISTING_ID_TO_EXPIRE)
         )
    DELETE
    FROM pictures
    WHERE pictures.id IN (SELECT id FROM PICTURES_TO_DELETE_IDS);
END;
$$
    LANGUAGE PLPGSQL;
-- END TRANSACTION 3

-- TRANSACTION 4
-- DROP FUNCTION transaction_4;
CREATE OR REPLACE FUNCTION transaction_4(u_id INTEGER)
    RETURNS TABLE
            (
                user_id            INTEGER,
                bid_year           DOUBLE PRECISION,
                bid_month          DOUBLE PRECISION,
                opened_count       NUMERIC,
                participated_count BIGINT,
                bids_count         BIGINT,
                mean               INTERVAL,
                average_bid        NUMERIC,
                avg_outbid         TEXT,
                avg_money          MONEY
            )
AS
$$
BEGIN
    RETURN QUERY WITH LISTINGS_OPENED AS (
        SELECT u_id                              AS user_id,
               COUNT(listings.id)                AS opened_count,
               extract(YEAR FROM bids.bid_time)  AS bid_year,
               extract(MONTH FROM bids.bid_time) AS bid_month
        FROM listings
                 JOIN bids ON listings.id = bids.listing_id
        WHERE seller_id = u_id
        GROUP BY seller_id, bid_year, bid_month
    ),
                      LISTINGS_PARTICIPATED AS (
                          SELECT u_id                              AS user_id,
                                 COUNT(listing_id)                 AS participated_count,
                                 extract(YEAR FROM bids.bid_time)  AS bid_year,
                                 extract(MONTH FROM bids.bid_time) AS bid_month
                          FROM users
                                   JOIN bids ON bids.bidder_id = users.id
                          WHERE users.id = u_id
                          GROUP BY users.id, bids.bidder_id, bids.bid_time
                      ),
                      NUMBER_OF_BIDS AS (
                          SELECT bids.bidder_id                    AS user_id,
                                 COUNT(bids.id)                    AS bids_count,
                                 extract(YEAR FROM bids.bid_time)  AS bid_year,
                                 extract(MONTH FROM bids.bid_time) AS bid_month
                          FROM bids
                          WHERE bids.bidder_id = u_id
                          GROUP BY bids.bidder_id, bids.bid_time
                      ),
                      MEAN_TIME_BETWEEN_BIDS AS (
                          SELECT u_id                         AS user_id,
                                 AVG(bid_interval)            AS mean,
                                 bids_with_interval.bid_year  AS bid_year,
                                 bids_with_interval.bid_month AS bid_month
                          FROM (
                                   SELECT bids.bidder_id,
                                          bids.bid_time - lag(bids.bid_time) OVER (ORDER BY bids.bid_time) AS bid_interval,
                                          extract(YEAR FROM bids.bid_time)                                 AS bid_year,
                                          extract(MONTH FROM bids.bid_time)                                AS bid_month
                                   FROM bids
                                   GROUP BY bids.bidder_id, bid_year, bid_month, bid_time
                               ) AS bids_with_interval
                          WHERE bids_with_interval.bidder_id = u_id
                          GROUP BY bids_with_interval.bidder_id, bids_with_interval.bid_year,
                                   bids_with_interval.bid_month
                      ),
                      AVG_BID_PRICE AS (
                          SELECT u_id                              AS user_id,
                                 AVG(bids.bid_price::NUMERIC)      AS average_bid,
                                 extract(YEAR FROM bids.bid_time)  AS bid_year,
                                 extract(MONTH FROM bids.bid_time) AS bid_month
                          FROM bids
                          WHERE bids.bidder_id = u_id
                          GROUP BY bids.bid_time, bids.bidder_id
                      ),
                      AVG_MINMAL_PRICE AS (
                          SELECT u_id                              AS user_id,
                                 CONCAT(
                                         ROUND(
                                                 AVG(((bids.bid_price::NUMERIC - l.minimal_price::NUMERIC) /
                                                      l.minimal_price::NUMERIC) / 100), 2
                                             )::VARCHAR, '%')      AS avg_outbid,
                                 extract(YEAR FROM bids.bid_time)  AS bid_year,
                                 extract(MONTH FROM bids.bid_time) AS bid_month
                          FROM bids
                                   JOIN listings l ON bids.listing_id = l.id
                          WHERE bids.bidder_id = u_id
                          GROUP BY bids.bid_time, bids.listing_id, bids.bidder_id
                      ),
                      AVG_MONEY_SPENT AS (
                          SELECT u_id                                                                             AS user_id,
                                 AVG(
                                         sales.sale_price::NUMERIC + sales.marketplace_brokerage::NUMERIC)::MONEY AS avg_money,
                                 extract(YEAR FROM sales.payment_time)                                            AS bid_year,
                                 extract(MONTH FROM sales.payment_time)                                           AS bid_month
                          FROM sales
                          WHERE sales.listing_id IN (
                              SELECT listings.id
                              FROM listings
                                       JOIN bids ON listings.id = bids.listing_id
                              WHERE bids.bid_status = 'winner'
                                AND bids.bidder_id = u_id
                              GROUP BY listings.id
                          )
                          GROUP BY sales.payment_time
                      )
                 SELECT users.id                                        AS id,
                        LISTINGS_OPENED.bid_year                        AS bid_year,
                        LISTINGS_OPENED.bid_month                       AS bid_month,
                        SUM(LISTINGS_OPENED.opened_count)               AS opened_count,
                        COUNT(LISTINGS_PARTICIPATED.participated_count) AS participated_count,
                        COUNT(NUMBER_OF_BIDS.bids_count)                AS bids_count,
                        MEAN_TIME_BETWEEN_BIDS.mean                     AS mean,
                        AVG_BID_PRICE.average_bid                       AS average_bid,
                        AVG_MINMAL_PRICE.avg_outbid                     AS avg_outbid,
                        AVG_MONEY_SPENT.avg_money                       AS avg_money
                 FROM users
                          LEFT JOIN LISTINGS_OPENED ON LISTINGS_OPENED.user_id = users.id
                          LEFT JOIN LISTINGS_PARTICIPATED
                                    ON LISTINGS_PARTICIPATED.bid_year = LISTINGS_OPENED.bid_year AND
                                       LISTINGS_PARTICIPATED.bid_month = LISTINGS_OPENED.bid_month
                          LEFT JOIN NUMBER_OF_BIDS ON NUMBER_OF_BIDS.bid_year = LISTINGS_OPENED.bid_year AND
                                                      NUMBER_OF_BIDS.bid_month = LISTINGS_OPENED.bid_month
                          LEFT JOIN MEAN_TIME_BETWEEN_BIDS
                                    ON MEAN_TIME_BETWEEN_BIDS.bid_year = LISTINGS_OPENED.bid_year AND
                                       MEAN_TIME_BETWEEN_BIDS.bid_month = LISTINGS_OPENED.bid_month
                          LEFT JOIN AVG_BID_PRICE ON AVG_BID_PRICE.bid_year = LISTINGS_OPENED.bid_year AND
                                                     AVG_BID_PRICE.bid_month = LISTINGS_OPENED.bid_month
                          LEFT JOIN AVG_MINMAL_PRICE ON AVG_MINMAL_PRICE.bid_year = LISTINGS_OPENED.bid_year AND
                                                        AVG_MINMAL_PRICE.bid_month = LISTINGS_OPENED.bid_month
                          LEFT JOIN AVG_MONEY_SPENT ON AVG_MONEY_SPENT.bid_year = LISTINGS_OPENED.bid_year AND
                                                       AVG_MONEY_SPENT.bid_month = LISTINGS_OPENED.bid_month
                 WHERE users.id = u_id
                 GROUP BY users.id,
                          LISTINGS_OPENED.bid_year,
                          LISTINGS_OPENED.bid_month,
                          MEAN_TIME_BETWEEN_BIDS.mean,
                          AVG_BID_PRICE.average_bid,
                          AVG_MINMAL_PRICE.avg_outbid,
                          AVG_MONEY_SPENT.avg_money;
END;
$$
    LANGUAGE PLPGSQL;
-- END TRANSACTION 4

-- TRANSACTION 5
CREATE OR REPLACE FUNCTION transaction_5(p_time_from TIMESTAMP,
                                         p_time_to TIMESTAMP)
    RETURNS TABLE
            (
                listing_id                                 INT,
                seller_id                                  INT,
                minimal_price                              MONEY,
                highest_bid                                MONEY,
                average_bid                                MONEY,
                number_of_bids                             INT,
                latest_bid_before_closing                  TIMESTAMP,
                latest_bid_before_closing_time_remaining   INTERVAL,
                number_of_pictures_to_number_of_bids_ratio FLOAT,
                number_of_bidders                          INT

            )
AS
$$
DECLARE
BEGIN
    RETURN QUERY
        SELECT b.listing_id,
               l.seller_id,
               l.minimal_price,
               max(bid_price),
               avg(bid_price::NUMERIC)::MONEY,
               count(b.*)::INT,
               max(bid_time),
               min(l.closing_time - b.bid_time),
               (count(lp.picture_id) / count(b.*))::FLOAT,
               count(b.bidder_id)::INT
        FROM bids b
                 JOIN listings l ON b.listing_id = l.id
                 JOIN listings_pictures lp ON l.id = lp.listing_id
        WHERE b.bid_time BETWEEN p_time_from AND p_time_to
        GROUP BY 1, 2, 3;
END ;
$$
    LANGUAGE plpgsql;

-- END TRANSACTION 5

-- Timed Execution wrappers

CREATE OR REPLACE FUNCTION calc_time(start_time TIMESTAMPTZ,
                                     end_time TIMESTAMPTZ)
    RETURNS NUMERIC AS
$$
BEGIN
    RETURN 1000 * (EXTRACT(EPOCH FROM end_time)::NUMERIC - EXTRACT(EPOCH FROM start_time)::NUMERIC);
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION timed_execution_1() RETURNS INTEGER AS
$$
DECLARE
    start_time TIMESTAMPTZ := clock_timestamp();
BEGIN
    PERFORM transaction_1(
            'random description'::TEXT,
            '$20.30'::MONEY,
            clock_timestamp()::TIMESTAMP,
            (clock_timestamp()::TIMESTAMP + '30 days'::INTERVAL)::TIMESTAMP,
            200000::INT
        );
    RETURN calc_time(start_time, clock_timestamp());
END;
$$
    LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION timed_execution_2() RETURNS INTEGER AS
$$
DECLARE
    start_time TIMESTAMPTZ := clock_timestamp();
BEGIN
    PERFORM transaction_2(10);
    RETURN calc_time(start_time, clock_timestamp());
END
$$
    LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION timed_execution_3() RETURNS INTEGER AS
$$
DECLARE
    start_time TIMESTAMPTZ := clock_timestamp();
BEGIN
    PERFORM transaction_3();
    RETURN calc_time(start_time, clock_timestamp());
END;
$$
    LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION timed_execution_4() RETURNS INTEGER AS
$$
DECLARE
    start_time TIMESTAMPTZ := clock_timestamp();
BEGIN
    PERFORM transaction_4(100);
    RETURN calc_time(start_time, clock_timestamp());
END;
$$
    LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION timed_execution_5() RETURNS INTEGER AS
$$
DECLARE
    start_time TIMESTAMPTZ := clock_timestamp();
BEGIN
    PERFORM transaction_5((now() - INTERVAL '1 year')::TIMESTAMP, now()::TIMESTAMP);
    RETURN calc_time(start_time, clock_timestamp());
END;
$$
    LANGUAGE PLPGSQL;

-- END Timed Execution wrappers


-- MEASURE

-- this is used to store execution time in measure table using autonomous transaction
-- take note that it is necessary to create such table (preferably in another database and schema, but can be in the same)
CREATE EXTENSION dblink;
CREATE SCHEMA measures;
CREATE TABLE measures.measures
(
    id               SERIAL NOT NULL,
    measure_time_ms  NUMERIC(1000),
    transaction_name VARCHAR,
    improvement_name TEXT
);

CREATE OR REPLACE FUNCTION log_dblink(v NUMERIC, t TEXT, i TEXT)
    RETURNS VOID
    LANGUAGE SQL
AS
$$
    -- change dbname to postgres and measures.measures to public.measures if measures table is in the same schema
SELECT dblink('host=/var/run/postgresql port=5432 user=postgres',
              FORMAT(
                      'INSERT INTO measures.measures (measure_time_ms, transaction_name, improvement_name) VALUES (%L, %L, %L)',
                      v, t, i)
           );
$$;

CREATE OR REPLACE PROCEDURE destroy_buffers()
    LANGUAGE plpgsql AS
$$
DECLARE
    row RECORD;
BEGIN
    FOR i IN 1..(
                    SELECT setting::BIGINT
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
