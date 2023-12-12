SQL Query = SELECT customer_id, SUM(amount) AS total_spending
FROM transactions WHERE transaction_date >= DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR) GROUP BY customer_id ORDER BY total_spending DESC LIMIT 3;
