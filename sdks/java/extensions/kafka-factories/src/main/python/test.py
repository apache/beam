import apache_beam as beam
import os
from apache_beam.options.pipeline_options import (
  PipelineOptions,
  GoogleCloudOptions,
  StandardOptions,
)
from apache_beam.io.kafka import ReadFromKafka
# Import Beam testing utilities
from apache_beam.testing.util import assert_that, equal_to

# Deserializers are specified by their full Java class path in the Python SDK
# because it uses a Java expansion service.
from apache_beam.io.kafka import ReadFromKafka
from apache_beam import coders

"""
A simple Beam pipeline that reads records from a Kafka topic and prints them to the console.
This version is configured for Kerberos authentication and to run on Dataflow.

This script is set up to run as part of a test, with hard-coded variables
instead of command-line arguments.
"""

def run():

  # --- 1. Define Hard-Coded Test Variables ---
  # TODO: Update these placeholders for your test environment

  # Dataflow Options
  RUNNER = 'DataflowRunner' # or 'DirectRunner' for local testing
  PROJECT_ID = 'dataflow-testing-311516'
  REGION = 'us-central1'
  TEMP_LOCATION = 'gs://fozzie_testing_bucket/temp/'
  VPC = 'fozzie-test-vpc'
  VPC_SUBNET = 'regions/us-central1/subnetworks/fozzie-test-vpc-subnet'

  # Kafka Connection
  KAFKA_BOOTSTRAP_SERVER = 'fozzie-test-kafka-broker.us-central1-c.c.dataflow-testing-311516.internal:9092'
  KAFKA_TOPIC = 'fozzie_test_kerberos_topic'

  # Kerberos
  KEYTAB_FILE_PATH = 'secretValue:projects/dataflow-testing-311516/secrets/kafka-client-keytab/versions/latest'
  PRINCIPAL = 'kafka-client@US-CENTRAL1-B.C.DATAFLOW-TESTING-311516.INTERNAL'

  # --- 2. Set up pipeline options from variables ---
  options_dict = {
    'runner': RUNNER,
    'project': PROJECT_ID,
    'region': REGION,
    'temp_location': TEMP_LOCATION,
    'network': VPC,
    'subnetwork': VPC_SUBNET,
    'streaming': True,
    'sdk_location': '/home/fozzie_google_com/beam_playground/beam/sdks/python/build/apache_beam-2.69.0.dev0.tar.gz',
    'sdk_harness_container_image_overrides': '.*java.*,us-central1-docker.pkg.dev/dataflow-testing-311516/fozzie/kerb-test-image'
  }
  options = PipelineOptions.from_dictionary(options_dict)

  # --- 3. Define Kerberos configuration ---
  # This config will point to the keytab file *on the worker*
  JAAS_CONFIG = (
    f'com.sun.security.auth.module.Krb5LoginModule required '
    f'useTicketCache=false useKeyTab=true storeKey=true '
    f'keyTab="{KEYTAB_FILE_PATH}" '
    f'principal="{PRINCIPAL}";'
  )

  # --- 4. Define Expected Records for Assertion ---
  # Create a list: ['test1', 'test2', ..., 'test11']
  expected_records = [f'test{i}' for i in range(12, 23)]

  # --- 5. Create and run the pipeline ---
  with beam.Pipeline(options=options) as p:

    print("Starting Dataflow pipeline to read from Kafka with Kerberos auth...")

    records = (
      p
      # --- 6. Apply the ReadFromKafka transform ---
      | "Read from Kafka" >> ReadFromKafka(
      consumer_config={
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVER,
        # This is the CRITICAL part for reading existing records.
        "auto.offset.reset": "earliest",

        # --- Kerberos Configuration ---
        "security.protocol": "SASL_PLAINTEXT", # Or SASL_SSL if you use SSL
        "sasl.mechanism": "GSSAPI",
        "sasl.kerberos.service.name": "kafka", # This must match your Kafka broker's config
        "sasl.jaas.config": JAAS_CONFIG
      },
      topics=[KAFKA_TOPIC],

      # We *must* make the source bounded for an assertion to work.
      max_num_records=11,
      consumer_factory_fn_class="org.apache.beam.sdk.extensions.kafka.factories.KerberosConsumerFactoryFn",
      consumer_factory_fn_params={"krb5Location": "gs://fozzie_testing_bucket/kerberos/krb5.conf"}
    )

      # The read transform returns KafkaRecord named tuples.
      # We'll extract just the value.
      | "Extract and Decode Value" >> beam.Map(lambda record: record[1].decode('utf-8'))
    )

    # --- 7. Assert the records match the expected list ---
    # This will cause the pipeline to fail if the records do not match.
    assert_that(
      records,
      equal_to(expected_records)
    )

if __name__ == "__main__":
  run()

