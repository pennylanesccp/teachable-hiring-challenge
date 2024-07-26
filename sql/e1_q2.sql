-- QUERY FOR THE SECOND QUESTION OF THE FIRST EXERCISE

WITH ProducerRevenue AS (
    SELECT 
        p.producer_id
        , pi.product_id
        , SUM(pi.item_quantity * pi.purchase_value) AS total_revenue
        , ROW_NUMBER() OVER (PARTITION BY p.producer_id ORDER BY SUM(pi.item_quantity * pi.purchase_value) DESC) AS revenue_rank

    FROM 
        purchase p

    JOIN 
        product_item pi ON p.prod_item_id = pi.prod_item_id

    GROUP BY 
        p.producer_id
        , pi.product_id
)

SELECT 
    producer_id
    , product_id
    , total_revenue

FROM 
    ProducerRevenue

WHERE 
    revenue_rank <= 2

ORDER BY 
    producer_id
    , revenue_rank