SELECT
    transaction_date
    , subsidiary
    , SUM(purchase_value) AS daily_gmv

FROM
    purchase_log_history

WHERE
    is_active = 1
    AND release_date IS NOT NULL

GROUP BY
    transaction_date
    , subsidiary
    
ORDER BY
    transaction_date
    , subsidiary
;
