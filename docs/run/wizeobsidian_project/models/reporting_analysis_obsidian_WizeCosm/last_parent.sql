
  
    

  create  table "wizecosm_NAS"."wizeobsidian"."last_parent__dbt_tmp"
  
  
    as
  
  (
    

WITH data AS (
    SELECT
        folder1,
        folder2,
        folder3,
        folder4,
        folder5,
        file_name,
        is_more_than_20_char,
        update_date,
        CASE
            WHEN folder5 = file_name THEN folder4
            WHEN folder4 = file_name THEN folder3
            WHEN folder3 = file_name THEN folder2
            WHEN folder2 = file_name THEN folder1
            ELSE NULL
        END AS last_parent
    FROM wizeobsidian.md_file_analysis
)
SELECT
    last_parent,
    file_name,
    is_more_than_20_char,
    update_date
FROM data
WHERE last_parent IS NOT NULL
  );
  