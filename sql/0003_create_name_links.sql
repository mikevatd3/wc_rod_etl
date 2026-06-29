ALTER TABLE rod.parties ADD COLUMN naive_name_dedupe INTEGER;

WITH ranked AS (
    SELECT DISTINCT
        TRIM(combined_name) AS trimmed,
        DENSE_RANK() OVER (ORDER BY TRIM(combined_name)) as value_id
    FROM rod.parties
)
UPDATE rod.parties p
SET naive_name_dedupe = r.value_id
FROM ranked r
WHERE TRIM(p.combined_name) = r.trimmed;

