import subprocess
import pytest
import json
import os
import time
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer, Consumer, KafkaError
from testcontainers.kafka import KafkaContainer
from pyspark.sql import SparkSession
from shutil import rmtree

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


def produce_messages():
    producer = Producer({"bootstrap.servers": kafka.get_bootstrap_server()})
    messages = [{"id": 1, "value": "foo"}, {"id": 2, "value": "bar"}]
    for message in messages:
        producer.produce("json_stream", value=json.dumps(message).encode("utf-8"))
    producer.flush()


def run_dbt():
    subprocess.run(["dbt", "run"], capture_output=False)


def read_results_from_spark(spark: SparkSession):
    spark.sql("SHOW SCHEMAS").show(100, False)
    spark.sql("SHOW TABLES IN experiments").show(100, False)
    spark.sql("DESC EXTENDED kafka_source_table").show(100, False)
    df = spark.sql("SELECT * FROM experiments.kafka_direct_model")
    results = df.collect()
    spark.stop()
    return results


def test_dbt_spark_pipeline(spark_session: SparkSession):
    #spark_session.sql("DROP DATABASE IF EXISTS experiments CASCADE")
    # clean_kafka_topic('foo_stream')
    print("Starting")
    produce_messages()
    print("Produced messages")
    run_dbt()
    print("Ran dbt")
    time.sleep(10)  # Wait for dbt to finish processing
    print("Waited for dbt")
    results = read_results_from_spark(spark_session)
    table_names = [row.tableName for row in results]
    assert len(table_names) == 2
    assert {"id": 1, "value": "foo"} in results
    assert {"id": 2, "value": "bar"} in results


if __name__ == "__main__":
    pytest.main([__file__])
