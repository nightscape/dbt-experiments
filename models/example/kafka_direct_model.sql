{{ config(materialized='table') }}
WITH kafka_data AS (
    SELECT
        from_avro(value, '{ "type": "record", "name": "MyAvroRecord", "namespace": "com.example", "fields": [ { "name": "id", "type": "int" }, { "name": "value", "type": "string" } ] }', null) AS parsed_value
    FROM
        {{ source('experiments', 'kafka_source_table') }}
)

SELECT
    parsed_value.*
FROM
    kafka_data
