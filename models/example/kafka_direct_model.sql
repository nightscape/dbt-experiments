WITH kafka_data AS (
    SELECT
        from_json(CAST(value AS STRING), 'struct<id:int,value:string>') AS json_value
    FROM
        {{ source('default', 'kafka_source_table') }}
)

SELECT
    json_value.id,
    json_value.value
FROM
    kafka_data
