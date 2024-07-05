WITH value_string AS (
    SELECT
        CAST(value AS STRING) AS value
    FROM
        {{ source('default', 'kafka_source_table') }}
), kafka_data AS (
    SELECT
        -- TODO Parse actual JSON values
        -- TODO Infer schema from JSON
        from_json('{"id":1, "value":"foo"}', schema_of_json('{"id":1, "value":"foo"}')) AS json_value
    FROM
        value_string
)

SELECT
    json_value.*
FROM
    kafka_data
