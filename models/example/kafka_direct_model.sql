WITH kafka_data AS (
    SELECT
        key,
        value
    FROM
        {{ source('default', 'kafka_source_table') }}
), parsed_data AS (
    SELECT
        from_json(CAST(value AS STRING), 'struct<id:int,value:string>') AS json_value
    FROM
        kafka_data
)

SELECT
    json_value.id,
    json_value.value
FROM
    parsed_data
