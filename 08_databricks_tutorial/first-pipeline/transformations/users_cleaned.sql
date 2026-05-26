-- Drop all rows that do not have an email address

CREATE MATERIALIZED VIEW users_cleaned
(
  CONSTRAINT non_null_email EXPECT (email IS NOT NULL) ON VIOLATION DROP ROW
) AS
SELECT *
FROM sample_users_first_pipeline;