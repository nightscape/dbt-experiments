{{ config(materialized='incremental') }}

WITH kafka_data AS (
    SELECT
        from_avro(value, '{ "type": "record", "name": "MyAvroRecord", "namespace": "com.example", "fields": [ { "name": "id", "type": "int" }, { "name": "value", "type": "string" } ] }', null) AS parsed_value,
        timestamp AS kafka_timestamp
    FROM
        {{ source('kafka', 'data_stream') }}
{% if is_incremental() %}
    WHERE
        timestamp > (SELECT COALESCE(MAX(kafka_timestamp), CAST('1970-01-01' AS TIMESTAMP)) FROM {{ this }})
{% endif %}
),

persisted_data AS (
    SELECT
        id,
        MIN(kafka_timestamp) AS first_seen_timestamp
    FROM
        {{ this }}
    GROUP BY
        id
)

SELECT
    kd.parsed_value.id,
    kd.parsed_value.value,
    kd.kafka_timestamp,
{% if is_incremental() %}
    COALESCE(pd.first_seen_timestamp, kd.kafka_timestamp) AS first_seen_timestamp
{% else %}
    kd.kafka_timestamp AS first_seen_timestamp
{% endif %}
FROM
    kafka_data kd
{% if is_incremental() %}
LEFT JOIN
    persisted_data pd ON kd.parsed_value.id = pd.id
{% endif %}
