EXPLAIN ANALYZE
SELECT 1                                                                            AS user_id,
       AVG(sales.sale_price::NUMERIC + sales.marketplace_brokerage::NUMERIC)::MONEY AS avg_money,
       extract(YEAR FROM sales.payment_time)                                        AS bid_year,
       extract(MONTH FROM sales.payment_time)                                       AS bid_month
FROM sales
WHERE sales.listing_id IN (
    SELECT listings.id
    FROM listings
             JOIN bids ON listings.id = bids.listing_id
    WHERE bids.bid_status = 'winner'
      AND bids.bidder_id = 1
    GROUP BY listings.id
)
GROUP BY sales.payment_time;
