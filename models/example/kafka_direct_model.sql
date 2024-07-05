WITH value_string AS (
    SELECT
        CAST(value AS STRING) AS value
    FROM
        {{ source('experiments', 'kafka_source_table') }}
), kafka_data AS (
    SELECT
        -- TODO Infer schema from JSON
        from_json(value, schema_of_json('{"id":0, "value":""}')) AS json_value
    FROM
        value_string
)

SELECT
    json_value.*
FROM
    kafka_data

