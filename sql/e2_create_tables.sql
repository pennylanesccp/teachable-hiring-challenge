-- DROPING EXISTING TABLES

DROP TABLE IF EXISTS purchase;

DROP TABLE IF EXISTS product_item;

DROP TABLE IF EXISTS purchase_extra_info;


-- CREATING TABLES

CREATE TABLE purchase (
    transaction_datetime DATETIME
    , transaction_date DATE
    , purchase_id INT
    , buyer_id INT
    , prod_item_id INT
    , order_date DATE
    , release_date DATE
    , producer_id INT
);

CREATE TABLE product_item (
    transaction_datetime DATETIME
    , transaction_date DATE
    , purchase_id INT
    , product_id INT
    , item_quantity INT
    , purchase_value DECIMAL(10 2)
);

CREATE TABLE purchase_extra_info (
    transaction_datetime DATETIME
    , transaction_date DATE
    , purchase_id INT
    , subsidiary VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS purchase_log_history (
    transaction_datetime DATETIME
    , purchase_id INT
);