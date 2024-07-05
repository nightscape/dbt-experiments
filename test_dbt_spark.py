import subprocess
import pytest
import json
import os
import time
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from testcontainers.kafka import KafkaContainer
from pyspark.sql import SparkSession
from shutil import rmtree
import avro.schema
from avro.io import DatumWriter
from io import BytesIO

kafka = KafkaContainer().with_kraft()


@pytest.fixture(scope="module", autouse=True)
def setup(request):
    rmtree("spark-warehouse", ignore_errors=True)
    rmtree("metastore_db", ignore_errors=True)
    kafka.start()

    def remove_container():
        kafka.stop()

    request.addfinalizer(remove_container)
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = kafka.get_bootstrap_server()


@pytest.fixture(scope="function", autouse=True)
def setup_data():
    pass


# def clean_kafka_topic(topic_name, kafka_broker='localhost:9092'):
#    admin_client = AdminClient({'bootstrap.servers': kafka_broker})
#
#    # Delete the topic
#    admin_client.delete_topics([topic_name])
#
#    # Ensure the topic is deleted
#    while topic_name in admin_client.list_topics().topics:
#        pass
#
#    # Recreate the topic
#    new_topic = NewTopic(topic=topic_name, num_partitions=1, replication_factor=1)
#    admin_client.create_topics([new_topic])
#
#    # Ensure the topic is created
#    while topic_name not in admin_client.list_topics().topics:
#        pass


def create_topic(topic_name):
    admin_client = AdminClient({'bootstrap.servers': kafka.get_bootstrap_server()})
    new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
    try:
        admin_client.create_topics([new_topic])
        print(f"Topic '{topic_name}' created successfully")
    except KafkaException as e:
        if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
            print(f"Topic '{topic_name}' already exists")
        else:
            raise

def produce_messages():
    create_topic("data_stream")
    producer = Producer({"bootstrap.servers": kafka.get_bootstrap_server()})
    
    # Load Avro schema
    schema = avro.schema.parse(open("my-avro-record.avsc", "rb").read())
    writer = DatumWriter(schema)
    
    messages = [{"id": 1, "value": "foo"}, {"id": 2, "value": "bar"}]
    for message in messages:
        try:
            # Serialize message to Avro
            bytes_writer = BytesIO()
            writer.write(message, avro.io.BinaryEncoder(bytes_writer))
            avro_bytes = bytes_writer.getvalue()
            
            producer.produce("data_stream", value=avro_bytes)
            print(f"Produced Avro message: {message}")
        except KafkaException as e:
            print(f"Failed to produce message: {e}")
    producer.flush()


def run_dbt():
    subprocess.run(["dbt", "run-operation", "stage_external_sources", "--vars", "ext_full_refresh: true"], capture_output=False)
    subprocess.run(["dbt", "run"], capture_output=False)


def read_results_from_spark(spark: SparkSession):
    spark.sql("SHOW SCHEMAS").show(100, False)
    spark.sql("SHOW TABLES IN experiments").show(100, False)
    spark.sql("DESC EXTENDED experiments.kafka_source_table").show(100, False)
    df = spark.sql("SELECT * FROM experiments.kafka_direct_model")
    results = df.collect()
    spark.stop()
    return [row.asDict() for row in results]


def test_dbt_spark_pipeline(spark_session: SparkSession):
    print("Starting")
    produce_messages()
    print("Produced messages")
    run_dbt()
    print("Ran dbt")
    time.sleep(5)  # Increase wait time to ensure dbt has finished processing
    print("Waited for dbt")
    results = read_results_from_spark(spark_session)
    print(f"Results: {results}")
    assert len(results) == 2, f"Expected 2 results, got {len(results)}"
    assert {"id": 1, "value": "foo"} in results, "Expected {'id': 1, 'value': 'foo'} in results"
    assert {"id": 2, "value": "bar"} in results, "Expected {'id': 2, 'value': 'bar'} in results"


if __name__ == "__main__":
    pytest.main([__file__])
