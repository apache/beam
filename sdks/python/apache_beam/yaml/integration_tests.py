#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Runs integration tests in the tests directory."""

import contextlib
import copy
import glob
import itertools
import logging
import os
import random
import sqlite3
import string
import unittest
import uuid

import mock
import mysql.connector
import psycopg2
import pytds
import sqlalchemy
import yaml
from google.cloud import pubsub_v1
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.google import PubSubContainer
from testcontainers.kafka import KafkaContainer
from testcontainers.mssql import SqlServerContainer
from testcontainers.mysql import MySqlContainer
from testcontainers.postgres import PostgresContainer

import apache_beam as beam
from apache_beam.io import filesystems
from apache_beam.io.gcp.bigquery_tools import BigQueryWrapper
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.io.gcp.spanner_wrapper import SpannerWrapper
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.utils import python_callable
from apache_beam.yaml import yaml_provider
from apache_beam.yaml import yaml_transform
from apache_beam.yaml.conftest import yaml_test_files_dir


@contextlib.contextmanager
def gcs_temp_dir(bucket):
  """Context manager to create and clean up a temporary GCS directory.

  Creates a unique temporary directory within the specified GCS bucket
  and yields the path. Upon exiting the context, the directory and its
  contents are deleted.

  Args:
    bucket (str): The GCS bucket name (e.g., 'gs://my-bucket').

  Yields:
    str: The full path to the created temporary GCS directory.
         Example: 'gs://my-bucket/yaml-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx'
  """
  gcs_tempdir = bucket + '/yaml-' + str(uuid.uuid4())
  yield gcs_tempdir
  filesystems.FileSystems.delete([gcs_tempdir])


@contextlib.contextmanager
def temp_spanner_table(project, prefix='temp_spanner_db_'):
  """Context manager to create and clean up a temporary Spanner database and 
  table.

  Creates a unique temporary Spanner database within the specified project
  and a predefined table named 'tmp_table' with columns ['UserId', 'Key'].
  It yields connection details for the created resources. Upon exiting the
  context, the temporary database (and its table) is deleted.

  Args:
    project (str): The Google Cloud project ID.
    prefix (str): A prefix to use for the temporary database name.
      Defaults to 'temp_spanner_db_'.

  Yields:
    list[str]: A list containing connection details:
      [project_id, instance_id, database_id, table_name, list_of_columns].
      Example: ['my-project', 'beam-test', 'temp_spanner_db_...', 'tmp_table', 
        ['UserId', 'Key']]
  """
  spanner_client = SpannerWrapper(project)
  spanner_client._create_database()
  instance = "beam-test"
  database = spanner_client._test_database
  table = 'tmp_table'
  columns = ['UserId', 'Key']
  logging.info("Created Spanner database: %s", database)
  try:
    yield [f'{project}', f'{instance}', f'{database}', f'{table}', columns]
  finally:
    logging.info("Deleting Spanner database: %s", database)
    spanner_client._delete_database()


@contextlib.contextmanager
def temp_bigquery_table(project, prefix='yaml_bq_it_'):
  """Context manager to create and clean up a temporary BigQuery dataset.

  Creates a unique temporary BigQuery dataset within the specified project.
  It yields a placeholder table name string within that dataset (e.g.,
  'project.dataset_id.tmp_table'). The actual table is expected to be
  created by the test using this context.

  Upon exiting the context, the temporary dataset and all its contents
  (including any tables created within it) are deleted.

  Args:
    project (str): The Google Cloud project ID.
    prefix (str): A prefix to use for the temporary dataset name.
      Defaults to 'yaml_bq_it_'.

  Yields:
    str: The full path for a temporary BigQuery table within the created
    dataset.
         Example: 'my-project.yaml_bq_it_a1b2c3d4e5f6...tmp_table'
  """
  bigquery_client = BigQueryWrapper()
  dataset_id = '%s_%s' % (prefix, uuid.uuid4().hex)
  bigquery_client.get_or_create_dataset(project, dataset_id)
  logging.info("Created dataset %s in project %s", dataset_id, project)
  yield f'{project}.{dataset_id}.tmp_table'
  request = bigquery.BigqueryDatasetsDeleteRequest(
      projectId=project, datasetId=dataset_id, deleteContents=True)
  logging.info("Deleting dataset %s in project %s", dataset_id, project)
  bigquery_client.client.datasets.Delete(request)


@contextlib.contextmanager
def temp_sqlite_database(prefix='yaml_jdbc_it_'):
  """Context manager to provide a temporary SQLite database via JDBC for
  testing.

  This function creates a temporary SQLite database file on the local
  filesystem. It establishes a connection using 'sqlite3', creates a predefined
  'tmp_table', and then yields a JDBC connection string suitable for use in
  tests that require a generic JDBC connection (specifically configured for 
  SQLite in this case).

  The SQLite database file is automatically cleaned up (closed and deleted)
  when the context manager exits.

  Args:
      prefix (str): A prefix to use for the temporary database file name.

  Yields:
      str: A JDBC connection string for the temporary SQLite database.
           Example format: "jdbc:sqlite:<path_to_db_file>"

  Raises:
      sqlite3.Error: If there's an error connecting to or interacting with
                     the SQLite database during setup.
      Exception: Any other exception encountered during the setup or cleanup
                 process.
  """
  conn = cursor = None
  try:
    # Establish connection to the temp file
    db_name = f'{prefix}{uuid.uuid4().hex}.db'
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Create a temp table for tests
    cursor.execute(
        '''
      CREATE TABLE tmp_table (
        value INTEGER PRIMARY KEY,
        rank INTEGER
      )
    ''')
    conn.commit()
    yield f'jdbc:sqlite:{db_name}'
  except (sqlite3.Error, Exception) as err:
    logging.error("Error interacting with temporary SQLite DB: %s", err)
    raise err
  finally:
    # Close connections
    if cursor:
      cursor.close()
    if conn:
      conn.close()
    try:
      if os.path.exists(db_name):
        os.remove(db_name)
    except Exception as err:
      logging.error("Error deleting temporary SQLite DB: %s", err)
      raise err


@contextlib.contextmanager
def temp_mysql_database():
  """Context manager to provide a temporary MySQL database for testing.

  This function utilizes the 'testcontainers' library to spin up a
  MySQL instance within a Docker container. It then connects
  to this temporary database using 'mysql.connector', creates a predefined
  'tmp_table', and yields a JDBC connection string suitable for use in tests.

  The Docker container and the database instance are automatically managed
  and torn down when the context manager exits.

  Yields:
      str: A JDBC connection string for the temporary MySQL database.
           Example format:
           "jdbc:mysql://<host>:<port>/<db_name>?
              user=<user>&password=<password>"

  Raises:
      mysql.connector.Error: If there's an error connecting to or interacting
                             with the MySQL database during setup.
      Exception: Any other exception encountered during the setup process.
  """
  with MySqlContainer() as mysql_container:
    try:
      # Make connection to temp database and create tmp table
      engine = sqlalchemy.create_engine(mysql_container.get_connection_url())
      with engine.begin() as connection:
        connection.execute(
            sqlalchemy.text(
                "CREATE TABLE tmp_table (value INTEGER, `rank` INTEGER);"))

      # Construct the JDBC url for connections later on by tests
      jdbc_url = (
          f"jdbc:mysql://{mysql_container.get_container_host_ip()}:"
          f"{mysql_container.get_exposed_port(mysql_container.port_to_expose)}/"
          f"{mysql_container.MYSQL_DATABASE}?"
          f"user={mysql_container.MYSQL_USER}&"
          f"password={mysql_container.MYSQL_PASSWORD}")

      yield jdbc_url
    except mysql.connector.Error as err:
      logging.error("Error interacting with temporary MySQL DB: %s", err)
      raise err


@contextlib.contextmanager
def temp_postgres_database():
  """Context manager to provide a temporary PostgreSQL database for testing.

  This function utilizes the 'testcontainers' library to spin up a
  PostgreSQL instance within a Docker container. It then connects
  to this temporary database using 'psycopg2', creates a predefined 'tmp_table',
  and yields a JDBC connection string suitable for use in tests.

  The Docker container and the database instance are automatically managed
  and torn down when the context manager exits.

  Yields:
      str: A JDBC connection string for the temporary PostgreSQL database.
           Example format:
           "jdbc:postgresql://<host>:<port>/<db_name>?
              user=<user>&password=<password>"

  Raises:
      psycopg2.Error: If there's an error connecting to or interacting with
                      the PostgreSQL database during setup.
      Exception: Any other exception encountered during the setup process.
  """
  default_port = 5432

  # Start the postgress container using testcontainers
  with PostgresContainer(port=default_port) as postgres_container:
    try:
      # Make connection to temp database and create tmp table
      engine = sqlalchemy.create_engine(postgres_container.get_connection_url())
      with engine.begin() as connection:
        connection.execute(
            sqlalchemy.text(
                "CREATE TABLE tmp_table (value INTEGER, rank INTEGER);"))

      # Construct the JDBC url for connections later on by tests
      jdbc_url = (
          f"jdbc:postgresql://{postgres_container.get_container_host_ip()}:"
          f"{postgres_container.get_exposed_port(default_port)}/"
          f"{postgres_container.POSTGRES_DB}?"
          f"user={postgres_container.POSTGRES_USER}&"
          f"password={postgres_container.POSTGRES_PASSWORD}")

      yield jdbc_url
    except (psycopg2.Error, Exception) as err:
      logging.error("Error interacting with temporary Postgres DB: %s", err)
      raise err


@contextlib.contextmanager
def temp_sqlserver_database():
  """Context manager to provide a temporary SQL Server database for testing.

  This function utilizes the 'testcontainers' library to spin up a
  Microsoft SQL Server instance within a Docker container. It then connects
  to this temporary database using 'pytds', creates a predefined 'tmp_table',
  and yields a JDBC connection string suitable for use in tests.

  The Docker container and the database instance are automatically managed
  and torn down when the context manager exits.

  Yields:
      str: A JDBC connection string for the temporary SQL Server database.
           Example format:
           "jdbc:sqlserver://<host>:<port>;
              databaseName=<db_name>;
              user=<user>;
              password=<password>;
              encrypt=false;
              trustServerCertificate=true"

  Raises:
      pytds.Error: If there's an error connecting to or interacting with
                     the SQL Server database during setup.
      Exception: Any other exception encountered during the setup process.
  """
  default_port = 1433

  # Start the sql server using testcontainers
  with SqlServerContainer(port=default_port,
                          dialect='mssql+pytds') as sqlserver_container:
    try:
      # Make connection to temp database and create tmp table
      engine = sqlalchemy.create_engine(
          sqlserver_container.get_connection_url())
      with engine.begin() as connection:
        connection.execute(
            sqlalchemy.text(
                "CREATE TABLE tmp_table (value INTEGER, rank INTEGER);"))

      # Construct the JDBC url for connections later on by tests
      # NOTE: encrypt=false and trustServerCertificate=true is generally
      # needed for test container connections without proper certificates setup
      jdbc_url = (
          f"jdbc:sqlserver://{sqlserver_container.get_container_host_ip()}:"
          f"{int(sqlserver_container.get_exposed_port(default_port))};"
          f"databaseName={sqlserver_container.SQLSERVER_DBNAME};"
          f"user={sqlserver_container.SQLSERVER_USER};"
          f"password={sqlserver_container.SQLSERVER_PASSWORD};"
          f"encrypt=true;"
          f"trustServerCertificate=true")

      yield jdbc_url
    except (pytds.Error, Exception) as err:
      logging.error("Error interacting with temporary SQL Server DB: %s", err)
      raise err


class OracleTestContainer(DockerContainer):
  """
    OracleTestContainer is an updated version of OracleDBContainer that goes
    ahead and sets the oracle password, waits for logs to establish that the 
    container is ready before calling get_exposed_port, and uses a more modern
    oracle driver.  
    """
  def __init__(self):
    super().__init__("gvenzl/oracle-xe:21-slim")
    self.with_env("ORACLE_PASSWORD", "oracle")
    self.with_exposed_ports(1521)

  def start(self):
    super().start()
    wait_for_logs(self, "DATABASE IS READY TO USE!", timeout=300)
    return self

  def get_connection_url(self):
    port = self.get_exposed_port(1521)
    return \
      f"oracle+oracledb://system:oracle@localhost:{port}/?service_name=XEPDB1"


@contextlib.contextmanager
def temp_oracle_database():
  """Context manager to provide a temporary Oracle database for testing.

  This function utilizes the 'testcontainers' library to spin up an
  Oracle Database instance within a Docker container. It then connects
  to this temporary database using 'oracledb', creates a predefined

  NOTE: A custom OracleTestContainer class was created due to the current
  version (OracleDBContainer) that calls get_exposed_port too soon causing the
  service to hang until timeout.

  Yields:
      str: A JDBC connection string for the temporary Oracle database.
           Example format:
           "jdbc:oracle:thin:system/oracle@localhost:{port}/XEPDB1"

  Raises:
      oracledb.Error: If there's an error connecting to or interacting with
                      the Oracle database during setup.
      Exception: Any other exception encountered during the setup process.
  """
  with OracleTestContainer() as oracle:
    engine = sqlalchemy.create_engine(oracle.get_connection_url())
    with engine.connect() as connection:
      connection.execute(
          sqlalchemy.text(
              """
                CREATE TABLE tmp_table (
                    value NUMBER PRIMARY KEY,
                    rank NUMBER
                )
            """))
      connection.commit()
    port = oracle.get_exposed_port(1521)
    yield f"jdbc:oracle:thin:system/oracle@localhost:{port}/XEPDB1"


@contextlib.contextmanager
def temp_kafka_server():
  """Context manager to provide a temporary Kafka server for testing.

  This function utilizes the 'testcontainers' library to spin up a Kafka
  instance within a Docker container. It then yields the bootstrap server
  string, which can be used by Kafka clients to connect to this temporary
  server.

  The Docker container and the Kafka instance are automatically managed
  and torn down when the context manager exits.

  Yields:
      str: The bootstrap server string for the temporary Kafka instance.
           Example format: "localhost:XXXXX" or "PLAINTEXT://localhost:XXXXX"

  Raises:
      Exception: If there's an error starting the Kafka container or
                 interacting with the temporary Kafka server.
  """
  try:
    with KafkaContainer() as kafka_container:
      yield kafka_container.get_bootstrap_server()
  except Exception as err:
    logging.error("Error interacting with temporary Kakfa Server: %s", err)
    raise err


@contextlib.contextmanager
def temp_pubsub_emulator(project_id="apache-beam-testing"):
  """
  Context manager to provide a temporary Pub/Sub emulator for testing.

  This function uses 'testcontainers' to spin up a Google Cloud SDK
  container running the Pub/Sub emulator. It yields the emulator host
  string (e.g., "localhost:xxxxx") that can be used to configure Pub/Sub
  clients.

  The Docker container is automatically managed and torn down when the
  context manager exits.

  Args:
      project_id (str): The GCP project ID to be used by the emulator.
                        This doesn't need to be a real project for the emulator.

  Yields:
      str: The host and port for the Pub/Sub emulator (e.g., "localhost:xxxx").
            This will be the address to point your Pub/Sub client to.

  Raises:
      Exception: If the container fails to start or the emulator isn't ready.
  """
  with PubSubContainer(project=project_id) as pubsub_container:
    publisher = pubsub_v1.PublisherClient()
    random_front_charactor = random.choice(string.ascii_lowercase)
    topic_id = f"{random_front_charactor}{uuid.uuid4().hex[:8]}"
    topic_name_to_create = \
      f"projects/{pubsub_container.project}/topics/{topic_id}"
    created_topic_object = publisher.create_topic(name=topic_name_to_create)
    yield created_topic_object.name


def replace_recursive(spec, vars):
  if isinstance(spec, dict):
    return {
        key: replace_recursive(value, vars)
        for (key, value) in spec.items()
    }
  elif isinstance(spec, list):
    return [replace_recursive(value, vars) for value in spec]
  # TODO(https://github.com/apache/beam/issues/35067): Consider checking for
  # callable in the if branch above instead of checking lambda here.
  elif isinstance(
      spec, str) and '{' in spec and '{\n' not in spec and 'lambda' not in spec:
    try:
      return spec.format(**vars)
    except Exception as exn:
      raise ValueError(f"Error evaluating {spec}: {exn}") from exn
  else:
    return spec


def transform_types(spec):
  if spec.get('type', None) in (None, 'composite', 'chain'):
    if 'source' in spec:
      yield from transform_types(spec['source'])
    for t in spec.get('transforms', []):
      yield from transform_types(t)
    if 'sink' in spec:
      yield from transform_types(spec['sink'])
  else:
    yield spec['type']


def provider_sets(spec, require_available=False):
  """For transforms that are vended by multiple providers, yields all possible
  combinations of providers to use.
  """
  try:
    for p in spec['pipelines']:
      _ = yaml_transform.preprocess(copy.deepcopy(p['pipeline']))
  except Exception as exn:
    print(exn)
    all_transform_types = []
  else:
    all_transform_types = set.union(
        *(
            set(
                transform_types(
                    yaml_transform.preprocess(copy.deepcopy(p['pipeline']))))
            for p in spec['pipelines']))

  def filter_to_available(t, providers):
    if t == 'LogForTesting':
      # Don't fan out to all the (many) possibilities for this one...
      return [
          p for p in providers if isinstance(p, yaml_provider.InlineProvider)
      ][:1]
    elif require_available:
      for p in providers:
        if not p.available():
          raise ValueError("Provider {p} required for {t} is not available.")
      return providers
    else:
      return [p for p in providers if p.available()]

  standard_providers = yaml_provider.standard_providers()
  multiple_providers = {
      t: filter_to_available(t, standard_providers[t])
      for t in all_transform_types
      if len(filter_to_available(t, standard_providers[t])) > 1
  }
  if not multiple_providers:
    yield 'only', standard_providers
  else:
    names, provider_lists = zip(*sorted(multiple_providers.items()))
    for ix, c in enumerate(itertools.product(*provider_lists)):
      yield (
          '_'.join(
              n + '_' + type(p.underlying_provider()).__name__
              for (n, p) in zip(names, c)) + f'_{ix}',
          dict(standard_providers, **{n: [p]
                                      for (n, p) in zip(names, c)}))


def create_test_methods(spec):
  for suffix, providers in provider_sets(spec):

    def test(self, providers=providers):  # default arg to capture loop value
      vars = {}
      with contextlib.ExitStack() as stack:
        stack.enter_context(
            mock.patch(
                'apache_beam.yaml.yaml_provider.standard_providers',
                lambda: providers))
        for fixture in spec.get('fixtures', []):
          vars[fixture['name']] = stack.enter_context(
              python_callable.PythonCallableWithSource.
              load_from_fully_qualified_name(fixture['type'])(
                  **yaml_transform.SafeLineLoader.strip_metadata(
                      fixture.get('config', {}))))
        for pipeline_spec in spec['pipelines']:
          with beam.Pipeline(options=PipelineOptions(
              pickle_library='cloudpickle',
              **yaml_transform.SafeLineLoader.strip_metadata(pipeline_spec.get(
                  'options', {})))) as p:
            yaml_transform.expand_pipeline(
                p, replace_recursive(pipeline_spec, vars))

    yield f'test_{suffix}', test


def parse_test_files(filepattern):
  for path in glob.glob(filepattern):
    with open(path) as fin:
      suite_name = os.path.splitext(os.path.basename(path))[0].title().replace(
          '-', '') + 'Test'
      print(path, suite_name)
      methods = dict(
          create_test_methods(
              yaml.load(fin, Loader=yaml_transform.SafeLineLoader)))
      globals()[suite_name] = type(suite_name, (unittest.TestCase, ), methods)


# Logging setup
logging.getLogger().setLevel(logging.INFO)

# Dynamically create test methods from the tests directory.
# yaml_test_files_dir comes from conftest.py and set by pytest_configure.
_test_files_dir = yaml_test_files_dir
_file_pattern = os.path.join(
    os.path.dirname(__file__), _test_files_dir, '*.yaml')
parse_test_files(_file_pattern)

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
