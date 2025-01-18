{{ 
    config(
        materialized = 'table'  
    ) 
}}

WITH last_parent_data AS (
    SELECT
        last_parent,
        file_name
    FROM {{ ref('last_parent') }}  
)
SELECT
    last_parent,
    COUNT(DISTINCT file_name) AS nb_note
FROM last_parent_data
GROUP BY last_parent
