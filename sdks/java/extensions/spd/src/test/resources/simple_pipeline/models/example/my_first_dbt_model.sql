WITH source_data AS (
    SELECT 1 AS id
    UNION ALL
    SELECT NULL AS id
)
SELECT * FROM source_data