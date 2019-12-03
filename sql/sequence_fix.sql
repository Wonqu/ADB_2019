BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE bids IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('bids_id_seq', COALESCE((SELECT MAX(id)+1 FROM bids), 1), false);
COMMIT;

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE listings IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('listings_id_seq', COALESCE((SELECT MAX(id)+1 FROM listings), 1), false);
COMMIT;

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE listings_pictures IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('listings_pictures_id_seq', COALESCE((SELECT MAX(id)+1 FROM listings_pictures), 1), false);
COMMIT;

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE pictures IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('pictures_id_seq', COALESCE((SELECT MAX(id)+1 FROM pictures), 1), false);
COMMIT;

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE sales IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('sales_id_seq', COALESCE((SELECT MAX(id)+1 FROM sales), 1), false);
COMMIT;

BEGIN;
-- protect against concurrent inserts while you update the counter
LOCK TABLE users IN EXCLUSIVE MODE;
-- Update the sequence
SELECT setval('users_id_seq', COALESCE((SELECT MAX(id)+1 FROM users), 1), false);
COMMIT;
