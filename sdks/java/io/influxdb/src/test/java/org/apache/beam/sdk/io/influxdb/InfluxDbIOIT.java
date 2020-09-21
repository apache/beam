/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.influxdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.influxdb.InfluxDbIO.DataSourceConfiguration;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * A test of {@link InfluxDbIO} on an independent InfluxDB instance. Run the docker container using
 * the following command
 *
 * <pre>
 * docker run -p 8086:8086 -p 2003:2003 -p 8083:8083 -e INFLUXDB_GRAPHITE_ENABLED=true -e INFLUXDB_USER=supersadmin -e INFLUXDB_USER_PASSWORD=supersecretpassword influxdb
 * </pre>
 *
 * <p>This test requires a running instance of InfluxDB.
 *
 * <pre>
 *  ./gradlew integrationTest -p sdks/java/io/influxdb -DintegrationTestPipelineOptions='[
 *  "--influxdburl=http://localhost:8086",
 *  "--infuxDBDatabase=mypass",
 *  "--username=supersadmin"
 *  "--password=supersecretpassword"
 *  "-databaseName=db1"]'
 *  --tests org.apache.beam.sdk.io.influxdb.InfluxDbIOIT
 *  -DintegrationTestRunner=direct
 * </pre>
 */
@RunWith(JUnit4.class)
public class InfluxDbIOIT {

  // private static InfluxDBPipelineOptions options;
  // @Rule public Pipeline writePipeline = TestPipeline.create();
  @Rule public final transient TestPipeline writePipeline = TestPipeline.create();
  @Rule public final transient TestPipeline readPipeline = TestPipeline.create();

  public static InfluxDBPipelineOptions options;

  static {
    PipelineOptionsFactory.register(InfluxDBPipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(InfluxDBPipelineOptions.class);
  }

  @After
  public void clear() {
    try (InfluxDB connection =
        InfluxDBFactory.connect(
            options.getInfluxDBURL(),
            options.getInfluxDBUserName(),
            options.getInfluxDBPassword())) {
      connection.query(new Query(String.format("DROP DATABASE %s", options.getDatabaseName())));
    }
  }

  @Before
  public void initTest() {
    try (InfluxDB connection =
        InfluxDBFactory.connect(
            options.getInfluxDBURL(),
            options.getInfluxDBUserName(),
            options.getInfluxDBPassword())) {
      connection.query(new Query(String.format("CREATE DATABASE %s", options.getDatabaseName())));
    }
  }

  @BeforeClass
  public static void setup() throws IOException {
    PipelineOptionsFactory.register(InfluxDBPipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(InfluxDBPipelineOptions.class);
  }

  public void createRetentionPolicyInDB(
      String dbName, InfluxDBPipelineOptions options, String retentionPolicyName) {
    try (InfluxDB connection =
        InfluxDBFactory.connect(
            options.getInfluxDBURL(),
            options.getInfluxDBUserName(),
            options.getInfluxDBPassword())) {
      connection.query(new Query(String.format("CREATE DATABASE %s", dbName)));
      connection.query(
          new Query(
              String.format(
                  "CREATE RETENTION POLICY %s ON %s DURATION 1d REPLICATION 1",
                  retentionPolicyName, dbName)));
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteAndRead() {

    final int numOfElementsToReadAndWrite = 2000;
    createRetentionPolicyInDB(options.getDatabaseName(), options, "autogen");
    String metricName = "testWriteAndRead";
    writePipeline
        .apply(
            "Generate data",
            Create.of(GenerateData.getMetric(metricName, numOfElementsToReadAndWrite)))
        .apply(
            "Write data to InfluxDB",
            InfluxDbIO.write()
                .withDataSourceConfiguration(
                    DataSourceConfiguration.create(
                        StaticValueProvider.of(options.getInfluxDBURL()),
                        StaticValueProvider.of(options.getInfluxDBUserName()),
                        StaticValueProvider.of(options.getInfluxDBPassword())))
                .withDatabase(options.getDatabaseName())
                .withConsistencyLevel(ConsistencyLevel.ANY)
                .withBatchSize(100)
                .withDisableCertificateValidation(true));
    writePipeline.run().waitUntilFinish();
    PCollection<String> values =
        readPipeline.apply(
            "Read all points in Influxdb",
            InfluxDbIO.read()
                .withDataSourceConfiguration(
                    DataSourceConfiguration.create(
                        StaticValueProvider.of(options.getInfluxDBURL()),
                        StaticValueProvider.of(options.getInfluxDBUserName()),
                        StaticValueProvider.of(options.getInfluxDBPassword())))
                .withDatabase(options.getDatabaseName())
                .withQuery("SELECT * FROM \"testWriteAndRead\"")
                .withDisableCertificateValidation(true));

    PAssert.thatSingleton(values.apply("Count All", Count.globally()))
        .isEqualTo((long) numOfElementsToReadAndWrite);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteAndReadWithSingleMetric() {
    final int noOfElementsToReadAndWrite = 2000;
    createRetentionPolicyInDB(options.getDatabaseName(), options, "autogen");
    String metricName = "test_m";
    writePipeline
        .apply(
            "Generate data",
            Create.of(GenerateData.getMetric(metricName, noOfElementsToReadAndWrite)))
        .apply(
            "Write data to InfluxDB",
            InfluxDbIO.write()
                .withDataSourceConfiguration(
                    DataSourceConfiguration.create(
                        StaticValueProvider.of(options.getInfluxDBURL()),
                        StaticValueProvider.of(options.getInfluxDBUserName()),
                        StaticValueProvider.of(options.getInfluxDBPassword())))
                .withDatabase(options.getDatabaseName())
                .withConsistencyLevel(ConsistencyLevel.ANY)
                .withBatchSize(100)
                .withDisableCertificateValidation(true));
    writePipeline.run().waitUntilFinish();
    PCollection<String> values =
        readPipeline.apply(
            "Read all points in InfluxDB",
            InfluxDbIO.read()
                .withDataSourceConfiguration(
                    DataSourceConfiguration.create(
                        StaticValueProvider.of(options.getInfluxDBURL()),
                        StaticValueProvider.of(options.getInfluxDBUserName()),
                        StaticValueProvider.of(options.getInfluxDBPassword())))
                .withDatabase(options.getDatabaseName())
                .withDisableCertificateValidation(true)
                .withMetric(metricName));

    PAssert.thatSingleton(values.apply("Count All", Count.globally()))
        .isEqualTo((long) noOfElementsToReadAndWrite);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteAndReadWithMultipleMetric() {
    final int numOfElementsToReadAndWrite = 2000;
    createRetentionPolicyInDB(options.getDatabaseName(), options, "autogen");
    List<String> metric = new ArrayList<>();
    metric.add("testWriteAndReadWithMultipleMetric1");
    metric.add("testWriteAndReadWithMultipleMetric2");
    writePipeline
        .apply(
            "Generate data",
            Create.of(GenerateData.getMultipleMetric(metric, numOfElementsToReadAndWrite)))
        .apply(
            "Write data to InfluxDB",
            InfluxDbIO.write()
                .withDataSourceConfiguration(
                    DataSourceConfiguration.create(
                        StaticValueProvider.of(options.getInfluxDBURL()),
                        StaticValueProvider.of(options.getInfluxDBUserName()),
                        StaticValueProvider.of(options.getInfluxDBPassword())))
                .withDatabase(options.getDatabaseName())
                .withConsistencyLevel(ConsistencyLevel.ANY)
                .withBatchSize(100)
                .withDisableCertificateValidation(true));
    writePipeline.run().waitUntilFinish();
    PCollection<String> values =
        readPipeline.apply(
            "Read all points in InfluxDB",
            InfluxDbIO.read()
                .withDataSourceConfiguration(
                    DataSourceConfiguration.create(
                        StaticValueProvider.of(options.getInfluxDBURL()),
                        StaticValueProvider.of(options.getInfluxDBUserName()),
                        StaticValueProvider.of(options.getInfluxDBPassword())))
                .withDatabase(options.getDatabaseName())
                .withDisableCertificateValidation(true)
                .withQuery(
                    "SELECT * FROM autogen.testWriteAndReadWithMultipleMetric1,autogen.testWriteAndReadWithMultipleMetric2"));
    PAssert.thatSingleton(values.apply("Count All", Count.globally()))
        .isEqualTo((long) numOfElementsToReadAndWrite * 2);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteAndReadWithSingleMetricWithCustomRetentionPolicy() {
    final int noOfElementsToReadAndWrite = 2000;
    createRetentionPolicyInDB(options.getDatabaseName(), options, "test_retention");
    String metricName = "test_4";
    writePipeline
        .apply(
            "Generate data",
            Create.of(GenerateData.getMetric(metricName, noOfElementsToReadAndWrite)))
        .apply(
            "Write data to InfluxDB",
            InfluxDbIO.write()
                .withDataSourceConfiguration(
                    DataSourceConfiguration.create(
                        StaticValueProvider.of(options.getInfluxDBURL()),
                        StaticValueProvider.of(options.getInfluxDBUserName()),
                        StaticValueProvider.of(options.getInfluxDBPassword())))
                .withDatabase(options.getDatabaseName())
                .withDisableCertificateValidation(true)
                .withBatchSize(100)
                .withConsistencyLevel(ConsistencyLevel.ANY)
                .withRetentionPolicy("test_retention"));

    writePipeline.run().waitUntilFinish();
    PCollection<String> values =
        readPipeline.apply(
            "Read all points in InfluxDB",
            InfluxDbIO.read()
                .withDataSourceConfiguration(
                    DataSourceConfiguration.create(
                        StaticValueProvider.of(options.getInfluxDBURL()),
                        StaticValueProvider.of(options.getInfluxDBUserName()),
                        StaticValueProvider.of(options.getInfluxDBPassword())))
                .withDatabase(options.getDatabaseName())
                .withDisableCertificateValidation(true)
                .withRetentionPolicy("test_retention")
                .withMetric(metricName));

    PAssert.thatSingleton(values.apply("Count All", Count.globally()))
        .isEqualTo((long) noOfElementsToReadAndWrite);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteAndReadWithSQLForMultipleMetric() {
    final int noOfElementsToReadAndWrite = 2;
    createRetentionPolicyInDB(options.getDatabaseName(), options, "test_rp");
    List<String> metrics = new ArrayList<>();
    metrics.add("testWriteAndReadWithSQLForMultipleMetric1");
    metrics.add("testWriteAndReadWithSQLForMultipleMetric2");
    writePipeline
        .apply(
            "Generate data",
            Create.of(GenerateData.getMultipleMetric(metrics, noOfElementsToReadAndWrite)))
        .apply(
            "Write data to InfluxDB",
            InfluxDbIO.write()
                .withDataSourceConfiguration(
                    DataSourceConfiguration.create(
                        StaticValueProvider.of(options.getInfluxDBURL()),
                        StaticValueProvider.of(options.getInfluxDBUserName()),
                        StaticValueProvider.of(options.getInfluxDBPassword())))
                .withDatabase(options.getDatabaseName())
                .withDisableCertificateValidation(true)
                .withBatchSize(100)
                .withConsistencyLevel(ConsistencyLevel.ANY)
                .withRetentionPolicy("test_rp"));
    writePipeline.run().waitUntilFinish();
    PCollection<String> valuesForTestm =
        readPipeline.apply(
            "Read all points in InfluxDB For testWriteAndReadWithSQLForMultipleMetric1 metric",
            InfluxDbIO.read()
                .withDataSourceConfiguration(
                    DataSourceConfiguration.create(
                        StaticValueProvider.of(options.getInfluxDBURL()),
                        StaticValueProvider.of(options.getInfluxDBUserName()),
                        StaticValueProvider.of(options.getInfluxDBPassword())))
                .withDatabase(options.getDatabaseName())
                .withDisableCertificateValidation(true)
                .withMetric("testWriteAndReadWithSQLForMultipleMetric1")
                .withRetentionPolicy("test_rp"));
    PCollection<String> valuesForTestm1 =
        readPipeline.apply(
            "Read all points in InfluxDB For testWriteAndReadWithSQLForMultipleMetric2 metric",
            InfluxDbIO.read()
                .withDataSourceConfiguration(
                    DataSourceConfiguration.create(
                        StaticValueProvider.of(options.getInfluxDBURL()),
                        StaticValueProvider.of(options.getInfluxDBUserName()),
                        StaticValueProvider.of(options.getInfluxDBPassword())))
                .withDatabase(options.getDatabaseName())
                .withDisableCertificateValidation(true)
                .withMetric("testWriteAndReadWithSQLForMultipleMetric2")
                .withRetentionPolicy("test_rp"));
    PAssert.thatSingleton(valuesForTestm1.apply("Count All For test_m1 metric", Count.globally()))
        .isEqualTo((long) noOfElementsToReadAndWrite);
    PAssert.thatSingleton(valuesForTestm.apply("Count All For test_m metric", Count.globally()))
        .isEqualTo((long) noOfElementsToReadAndWrite);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testZeroRecordFormNonExistentMetric() {
    final int numOfElementsToReadAndWrite = 0;
    createRetentionPolicyInDB(options.getDatabaseName(), options, "autogen");
    PCollection<String> values =
        readPipeline.apply(
            "Read points in Influxdb",
            InfluxDbIO.read()
                .withDataSourceConfiguration(
                    DataSourceConfiguration.create(
                        StaticValueProvider.of(options.getInfluxDBURL()),
                        StaticValueProvider.of(options.getInfluxDBUserName()),
                        StaticValueProvider.of(options.getInfluxDBPassword())))
                .withDatabase(options.getDatabaseName())
                .withQuery("SELECT * FROM \"non_existentMetric\"")
                .withDisableCertificateValidation(true));

    PAssert.thatSingleton(values.apply("Count All", Count.globally()))
        .isEqualTo((long) numOfElementsToReadAndWrite);
    readPipeline.run().waitUntilFinish();
  }
}
