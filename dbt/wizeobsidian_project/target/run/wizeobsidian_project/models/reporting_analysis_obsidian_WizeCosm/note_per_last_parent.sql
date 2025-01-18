
  
    

  create  table "wizecosm_NAS"."wizeobsidian"."note_per_last_parent__dbt_tmp"
  
  
    as
  
  (
    

WITH last_parent_data AS (
    SELECT
        last_parent,
        file_name
    FROM "wizecosm_NAS"."wizeobsidian"."last_parent"  
)
SELECT
    last_parent,
    COUNT(DISTINCT file_name) AS nb_note
FROM last_parent_data
GROUP BY last_parent
  );
  