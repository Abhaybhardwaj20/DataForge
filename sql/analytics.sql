
-- 1. Total number of orders
SELECT COUNT(*) AS total_orders
FROM orders;

-- 2. Total revenue from completed orders
SELECT SUM(quantity * price) AS total_revenue
FROM orders
WHERE status = 'Completed';

-- 3. Revenue by product
SELECT
    product,
    SUM(quantity * price) AS revenue
FROM orders
WHERE status = 'Completed'
GROUP BY product
ORDER BY revenue DESC;

-- 4. Order count by status
SELECT
    status,
    COUNT(*) AS order_count
FROM orders
GROUP BY status
ORDER BY order_count DESC;
