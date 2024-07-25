-- Insert values into purchase table
INSERT INTO purchase (transaction_datetime, transaction_date, purchase_id, buyer_id, prod_item_id, order_date, release_date, producer_id)
VALUES
('2023-01-20 22:00:00', '2023-01-20', 55, 15947, 5, '2023-01-20', '2023-01-20', 852852),
('2023-01-26 00:01:00', '2023-01-26', 56, 369798, 746520, '2023-01-25', NULL, 963963),
('2023-02-05 10:00:00', '2023-02-05', 55, 160001, 5, '2023-01-20', '2023-01-20', 852852),
('2023-02-26 03:00:00', '2023-02-26', 69, 160001, 18, '2023-02-26', '2023-02-28', 96967),
('2023-07-15 09:00:00', '2023-07-15', 55, 160001, 5, '2023-01-20', '2023-03-01', 852852);

-- Insert values into product_item table
INSERT INTO product_item (transaction_datetime, transaction_date, purchase_id, product_id, item_quantity, purchase_value)
VALUES
('2023-01-20 22:02:00', '2023-01-20', 55, 696969, 10, 50.00),
('2023-01-25 23:59:59', '2023-01-25', 56, 808080, 120, 2400.00),
('2023-02-26 03:00:00', '2023-02-26', 69, 373737, 2, 2000.00),
('2023-07-12 09:00:00', '2023-07-12', 55, 696969, 10, 55.00);

-- Insert values into purchase_extra_info table
INSERT INTO purchase_extra_info (transaction_datetime, transaction_date, purchase_id, subsidiary)
 VALUES
('2023-01-23 00:05:00', '2023-01-23', 55, 'nacional'),
('2023-01-25 23:59:59', '2023-01-25', 56, 'internacional'),
('2023-02-28 01:10:00', '2023-02-28', 69, 'nacional'),
('2023-03-12 07:00:00', '2023-03-12', 69, 'internacional');