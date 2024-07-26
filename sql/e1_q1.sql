-- QUERY FOR THE FIRST QUESTION OF THE FIRST EXERCISE

SELECT 
    p.producer_id
    , SUM(pi.item_quantity * pi.purchase_value) AS total_revenue

FROM 
    purchase p

JOIN 
    product_item pi ON p.prod_item_id = pi.prod_item_id

WHERE 
    strftime('%Y', p.order_date) = '2021' -- YEAR(p.order_date) = 2021

GROUP BY 
    p.producer_id

ORDER BY 
    total_revenue DESC
    
LIMIT 50