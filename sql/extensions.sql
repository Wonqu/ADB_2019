ALTER TABLE listings
    ADD COLUMN bid_info XML;

DO
$$
    DECLARE
        i BIGINT;
    BEGIN
        FOR i IN SELECT id FROM listings
            LOOP
                UPDATE listings
                SET bid_info = (SELECT xmlelement(NAME bids, (
                    SELECT xmlagg(
                                   xmlelement(NAME bid,
                                              XMLATTRIBUTES(id AS id),
                                              xmlforest(bidder_id, bid_price, bid_time, bid_status)
                                       )
                               )
                    FROM bids
                    WHERE listing_id = i)))
                WHERE id = i;
            END LOOP;
    END;
$$

