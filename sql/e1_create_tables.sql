-- DROPING EXISTING TABLES

DROP TABLE IF EXISTS purchase;

DROP TABLE IF EXISTS product_item;

DROP TABLE IF EXISTS order_transaction_cost_hist;

DROP TABLE IF EXISTS purchase_extra_info;


-- CREATING TABLES

CREATE TABLE purchase (
    purchase_id BIGINT PRIMARY KEY
    , buyer_id BIGINT
    , prod_item_id BIGINT
    , order_date DATE
    , release_date DATE
    , producer_id BIGINT
    , purchase_partition BIGINT
    , prod_item_partition BIGINT
);

CREATE TABLE product_item (
    prod_item_id BIGINT PRIMARY KEY
    , product_id BIGINT
    , item_quantity INT
    , purchase_value FLOAT
    , prod_item_partition BIGINT
);