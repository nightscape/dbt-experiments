{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='kafka_timestamp',
    partition_by='timestamp_hour',
    batch_size='hour',
    begin=(modules.datetime.datetime.now() - modules.datetime.timedelta(hours=1)).isoformat(),
    lookback=1,
    full_refresh=false
) }}


WITH kafka_data AS (
    SELECT
        from_avro(value, '{ "type": "record", "name": "MyAvroRecord", "namespace": "dev.mauch", "fields": [ { "name": "id", "type": "int" }, { "name": "value", "type": "string" } ] }', null) AS parsed_value,
        offset AS kafka_offset,
        timestamp AS kafka_timestamp
    FROM
        {{ source('kafka', 'data_stream') }}
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
    DATE_TRUNC('hour', kd.kafka_timestamp) AS timestamp_hour,
    kd.kafka_offset,
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
