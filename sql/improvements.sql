-- Measuring before improvements

BEGIN TRANSACTION;
SAVEPOINT before_experiment;
CALL measure_1('Before');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_2('Before');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_3('Before');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_4('Before');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_5('Before');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_1('Before');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_2('Before');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_3('Before');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_4('Before');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_5('Before');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_1('Before');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_2('Before');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_3('Before');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_4('Before');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_5('Before');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_1('Before');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_2('Before');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_3('Before');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_4('Before');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_5('Before');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_1('Before');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_2('Before');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_3('Before');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_4('Before');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_5('Before');
ROLLBACK;

-- IMPROVEMENTS
CREATE EXTENSION btree_gin;

BEGIN TRANSACTION;
-- IMPROVEMENT 3
-- rename current table to backup
ALTER TABLE users
    RENAME TO users_backup;

-- Create partitioned table
CREATE TABLE users
(
    id         SERIAL,
    name       VARCHAR(64),
    auth_type  "AUTH_TYPE",
    auth_id    TEXT,
    created_at TIMESTAMP,
    last_login TIMESTAMP,
    active     BOOLEAN,
    PRIMARY KEY (id, active)
) PARTITION BY LIST (active);

CREATE TABLE users_active PARTITION OF users FOR VALUES IN (TRUE);
CREATE TABLE users_inactive PARTITION OF users FOR VALUES IN (FALSE);
CREATE TABLE users_default PARTITION OF users DEFAULT;

-- copy data from old table
INSERT INTO users (SELECT * FROM users_backup);

-- IMPROVEMENT 4
-- rename current table to backup
ALTER TABLE bids
    RENAME TO bids_backup;

-- Create partitioned table
CREATE TABLE bids
(
    id            SERIAL,
    listing_id    INTEGER REFERENCES listings,
    bidder_id     INTEGER,
    bidder_active BOOLEAN,
    bid_price     MONEY,
    bid_time      TIMESTAMP,
    bid_status    "BID_STATUS",
    PRIMARY KEY (id, bid_time),
    CONSTRAINT fk_bidder FOREIGN KEY (bidder_id, bidder_active) REFERENCES users (id, active)
)
    PARTITION BY RANGE (bid_time);

CREATE TABLE bids_p_2018_11
    PARTITION OF bids
        FOR VALUES FROM ('2018-11-01') TO ('2018-12-01');

CREATE TABLE bids_p_2018_12
    PARTITION OF bids
        FOR VALUES FROM ('2018-12-01') TO ('2019-1-01');

CREATE TABLE bids_p_2019_1
    PARTITION OF bids
        FOR VALUES FROM ('2019-1-01') TO ('2019-2-01');

CREATE TABLE bids_p_2019_2
    PARTITION OF bids
        FOR VALUES FROM ('2019-2-01') TO ('2019-3-01');

CREATE TABLE bids_p_2019_3
    PARTITION OF bids
        FOR VALUES FROM ('2019-3-01') TO ('2019-4-01');

CREATE TABLE bids_p_2019_4
    PARTITION OF bids
        FOR VALUES FROM ('2019-4-01') TO ('2019-5-01');

CREATE TABLE bids_p_2019_5
    PARTITION OF bids
        FOR VALUES FROM ('2019-5-01') TO ('2019-6-01');

CREATE TABLE bids_p_2019_6
    PARTITION OF bids
        FOR VALUES FROM ('2019-6-01') TO ('2019-7-01');

CREATE TABLE bids_p_2019_7
    PARTITION OF bids
        FOR VALUES FROM ('2019-7-01') TO ('2019-8-01');

CREATE TABLE bids_p_2019_8
    PARTITION OF bids
        FOR VALUES FROM ('2019-8-01') TO ('2019-9-01');

CREATE TABLE bids_p_2019_9
    PARTITION OF bids
        FOR VALUES FROM ('2019-9-01') TO ('2019-10-01');

CREATE TABLE bids_p_2019_10
    PARTITION OF bids
        FOR VALUES FROM ('2019-10-01') TO ('2019-11-01');

CREATE TABLE bids_p_2019_11
    PARTITION OF bids
        FOR VALUES FROM ('2019-11-01') TO ('2019-12-01');

CREATE TABLE bids_p_default
    PARTITION OF bids
        DEFAULT;

-- Copy data from old bids to new bids
INSERT INTO bids(id, listing_id, bidder_id, bid_price, bid_time, bid_status, bidder_active) (
    SELECT bb.id, bb.listing_id, bb.bidder_id, bb.bid_price, bb.bid_time, bb.bid_status, u.active
    FROM bids_backup bb
             JOIN users u ON u.id = bb.bidder_id
);

-- IMPROVEMENT 1
CREATE INDEX idx_btree_bids_listing_id ON bids USING btree (listing_id);
CREATE INDEX idx_btree_bids_bidder_id ON bids USING btree (bidder_id);
CREATE INDEX idx_btree_listings_seller_id ON listings USING btree (seller_id);
CREATE INDEX idx_btree_listings_pictures_listing_id ON listings_pictures USING btree (listing_id);
CREATE INDEX idx_btree_listings_pictures_picture_id ON listings_pictures USING btree (picture_id);

-- IMPROVEMENT 2
ALTER TABLE bids
    ALTER COLUMN bid_status TYPE VARCHAR USING bid_status::TEXT;
CREATE INDEX idx_brin_bids_bid_status ON bids USING brin (bid_status);

-- IMPROVEMENT 5
CREATE INDEX index_bids_bidtime_year ON bids USING btree (extract(YEAR FROM bid_time));
CREATE INDEX index_bids_bidtime_month ON bids USING btree (extract(MONTH FROM bid_time));
COMMIT;

BEGIN TRANSACTION;
-- Measuring after improvements
SAVEPOINT before_experiment;
CALL measure_1('After');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_2('After');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_3('After');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_4('After');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_5('After');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_1('After');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_2('After');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_3('After');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_4('After');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_5('After');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_1('After');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_2('After');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_3('After');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_4('After');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_5('After');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_1('After');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_2('After');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_3('After');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_4('After');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_5('After');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_1('After');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_2('After');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_3('After');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_4('After');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_5('After');
ROLLBACK;

-- creating new tablespace

CREATE TABLESPACE new_tablespace OWNER postgres LOCATION '/Users/ilayda/Desktop/postgres';
ALTER TABLE users_inactive
    SET TABLESPACE new_tablespace;

BEGIN TRANSACTION;
SAVEPOINT before_experiment;
CALL measure_3('Partition experiment');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_3('Partition experiment');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_3('Partition experiment');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_3('Partition experiment');
ROLLBACK TO SAVEPOINT before_experiment;
CALL measure_3('Partition experiment');
ROLLBACK;

SELECT *
FROM pg_tablespace;
