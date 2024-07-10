import pytest
import os
import time
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer, KafkaError, KafkaException
from testcontainers.kafka import KafkaContainer
from pyspark.sql import SparkSession
from shutil import rmtree
import avro.schema
from avro.io import DatumWriter
from io import BytesIO
from dbt.cli.main import dbtRunner

# initialize
dbt = dbtRunner()

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

def produce_messages(messages):
    create_topic("data_stream")
    producer = Producer({"bootstrap.servers": kafka.get_bootstrap_server()})
    
    # Load Avro schema
    schema = avro.schema.parse(open("my-avro-record.avsc", "rb").read())
    writer = DatumWriter(schema)
    
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


def run_dbt(full_refresh=False):
    if full_refresh:
        dbt.invoke(["run-operation", "stage_external_sources", "--vars", "ext_full_refresh: true"], capture_output=False, check=True)
    dbt.invoke(["run"], capture_output=False, check=True)


def read_results_from_spark(spark: SparkSession):
    df = spark.sql("SELECT * FROM experiments.kafka_direct_model")
    results = df.collect()
    return [row.asDict() for row in results]


def test_dbt_spark_pipeline(spark_session: SparkSession):
    print("Starting")
    
    # Define test data for multiple runs
    test_data = [
        [{"id": 1, "value": "foo"}, {"id": 2, "value": "bar"}],
        [{"id": 3, "value": "baz"}, {"id": 4, "value": "qux"}],
        [{"id": 5, "value": "quux"}]
    ]
    
    expected_results = []
    
    for i, messages in enumerate(test_data):
        print(f"Run {i+1}")
        produce_messages(messages)
        print(f"Produced messages for run {i+1}")
        
        run_dbt(full_refresh=(i == 0))
        print(f"Ran dbt for run {i+1}")
        
        time.sleep(5)  # Ensure dbt has finished processing
        
        results = read_results_from_spark(spark_session)
        print(f"Results for run {i+1}: {results}")
        
        expected_results.extend(messages)
        
        assert len(results) == len(expected_results), f"Expected {len(expected_results)} results, got {len(results)}"
        for expected in expected_results:
            assert expected in results, f"Expected {expected} in results"
    
    print("All runs completed successfully")


if __name__ == "__main__":
    pytest.main([__file__])
