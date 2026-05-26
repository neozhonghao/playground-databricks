-- Get the top 100 users by number of bookings

CREATE OR REFRESH MATERIALIZED VIEW users_and_bookings AS
SELECT u.name AS name, COUNT(b.booking_id) AS booking_count
FROM users_cleaned u
JOIN samples.wanderbricks.bookings b ON u.user_id = b.user_id
GROUP BY u.name
ORDER BY booking_count DESC
LIMIT 100;