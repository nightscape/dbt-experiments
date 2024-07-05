WITH value_string AS (
    SELECT
        CAST(value AS STRING) AS value
    FROM
        {{ source('default', 'kafka_source_table') }}
), kafka_data AS (
    SELECT
        from_json(value, 'struct<id:int,value:string>') AS json_value
    FROM
        value_string
)

SELECT
    json_value.id,
    json_value.value
FROM
    kafka_data
