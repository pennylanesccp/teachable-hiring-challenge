INSERT INTO purchase (purchase_id, buyer_id, prod_item_id, order_date, release_date, producer_id, purchase_partition, prod_item_partition)
VALUES 
(55, 15947, 5, '2022-12-01', '2022-12-01', 852852, 5, 5)
, (56, 369798, 746520, '2022-12-25', '2022-12-25', 963963, 6, 0)
, (57, 147, 98736, '2021-07-03', '2021-07-03', 963963, 7, 6)
, (58, 986533, 6565, '2021-10-12', NULL, 200478, 8, 5);

INSERT INTO product_item (prod_item_id, product_id, item_quantity, purchase_value, prod_item_partition)
VALUES 
(1, 69, 5, 500.00, 5)
, (5, 69, 120, 1.00, 0)
, (98736, 37, 69, 25.00, 6)
, (3, 96, 369, 140.00, 5);