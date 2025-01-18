

WITH last_parent_data AS (
    SELECT
      *
    FROM "wizecosm_NAS"."wizeobsidian"."last_parent"
)
SELECT
    last_parent,
    COUNT(DISTINCT file_name) AS nb_note
FROM last_parent_data
WHERE 1=1
    AND is_more_than_20_char IS TRUE
GROUP BY last_parent