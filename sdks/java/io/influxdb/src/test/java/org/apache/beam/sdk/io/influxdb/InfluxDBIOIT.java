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

import java.util.Arrays;
import java.util.Collections;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * A test of {@link InfluxDbIO} on an independent InfluxDB instance.
 * Run the docker container using the following command
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
 *  "--password=supersecretpassword"]'
 *  --tests org.apache.beam.sdk.io.influxdb.InfluxDbIOIT
 *  -DintegrationTestRunner=direct
 * </pre>
 */
@RunWith(JUnit4.class)
public class InfluxDbIOIT {

  private static InfluxDBPipelineOptions options;

  @Rule public final TestPipeline writePipeline = TestPipeline.create();
  @Rule public final TestPipeline readPipeline = TestPipeline.create();

  /** InfluxDBIO options. */
  public interface InfluxDBPipelineOptions extends IOTestPipelineOptions {
    @Description("InfluxDB host (host name/ip address)")
    @Default.String("http://localhost:8086")
    String getInfluxDBURL();

    void setInfluxDBURL(String value);

    @Description("Username for InfluxDB")
    @Default.String("superadmin")
    String getInfluxDBUserName();

    void setInfluxDBUserName(String value);

    @Description("Password for InfluxDB")
    @Default.String("supersecretpassword")
    String getInfluxDBPassword();

    void setInfluxDBPassword(String value);

    @Description("InfluxDB database name")
    @Default.String("db3")
    String getDatabaseName();

    void setDatabaseName(String value);
  }

  @BeforeClass
  public static void setUp() {
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
      connection.query(new Query("DROP DATABASE \"" + options.getDatabaseName() + "\""));
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

  private void createRetentionPolicyInDB(String dbName, String retentionPolicyName) {
    try (InfluxDB connection =
        InfluxDBFactory.connect(
            options.getInfluxDBURL(),
            options.getInfluxDBUserName(),
            options.getInfluxDBPassword())) {
      connection.query(
          new Query(
              String.format(
                  "CREATE RETENTION POLICY %s ON %s DURATION 1d REPLICATION 1",
                  retentionPolicyName, dbName)));
    }
  }

  @Test
  public void testWriteAndRead() {
    final int numOfElementsToReadAndWrite = 2000;
    writePipeline
        .apply("Generate data", Create.of(GenerateData.getMetric(numOfElementsToReadAndWrite)))
        .apply(
            "Write data to InfluxDB",
            InfluxDbIO.write()
                .withConfiguration(
                    InfluxDbIO.DataSourceConfiguration.create(
                        options.getInfluxDBURL(),
                        options.getInfluxDBUserName(),
                        options.getInfluxDBPassword()))
                .withDatabase(options.getDatabaseName())
                .withSslInvalidHostNameAllowed(false)
                .withSslEnabled(false));
    writePipeline.run().waitUntilFinish();
    PCollection<String> values =
        readPipeline.apply(
            "Read all points in Influxdb",
            InfluxDbIO.read()
                .withDataSourceConfiguration(
                    InfluxDbIO.DataSourceConfiguration.create(
                        options.getInfluxDBURL(),
                        options.getInfluxDBUserName(),
                        options.getInfluxDBPassword()))
                .withDatabase(options.getDatabaseName())
                .withQuery("SELECT * FROM \"test_m\"")
                .withSslInvalidHostNameAllowed(false)
                .withSslEnabled(false));

    PAssert.thatSingleton(values.apply("Count All", Count.globally()))
        .isEqualTo((long) numOfElementsToReadAndWrite);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testWriteAndReadWithSingleMetric() {
    final int noOfElementsToReadAndWrite = 2000;
    writePipeline
        .apply("Generate data", Create.of(GenerateData.getMetric(noOfElementsToReadAndWrite)))
        .apply(
            "Write data to InfluxDB",
            InfluxDbIO.write()
                .withConfiguration(
                    InfluxDbIO.DataSourceConfiguration.create(
                        options.getInfluxDBURL(),
                        options.getInfluxDBUserName(),
                        options.getInfluxDBPassword()))
                .withDatabase(options.getDatabaseName())
                .withSslInvalidHostNameAllowed(false)
                .withSslEnabled(false));
    writePipeline.run().waitUntilFinish();
    PCollection<String> values =
        readPipeline.apply(
            "Read all points in InfluxDB",
            InfluxDbIO.read()
                .withDataSourceConfiguration(
                    InfluxDbIO.DataSourceConfiguration.create(
                        options.getInfluxDBURL(),
                        options.getInfluxDBUserName(),
                        options.getInfluxDBPassword()))
                .withDatabase(options.getDatabaseName())
                .withMetrics(Collections.singletonList("test_m"))
                .withSslInvalidHostNameAllowed(false)
                .withSslEnabled(false));

    PAssert.thatSingleton(values.apply("Count All", Count.globally()))
        .isEqualTo((long) noOfElementsToReadAndWrite);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testWriteAndReadWithMultipleMetric() {
    final int numOfElementsToReadAndWrite = 2000;
    writePipeline
        .apply(
            "Generate data", Create.of(GenerateData.getMultipleMetric(numOfElementsToReadAndWrite)))
        .apply(
            "Write data to InfluxDB",
            InfluxDbIO.write()
                .withConfiguration(
                    InfluxDbIO.DataSourceConfiguration.create(
                        options.getInfluxDBURL(),
                        options.getInfluxDBUserName(),
                        options.getInfluxDBPassword()))
                .withDatabase(options.getDatabaseName())
                .withSslInvalidHostNameAllowed(false)
                .withSslEnabled(false));
    writePipeline.run().waitUntilFinish();
    PCollection<String> values =
        readPipeline.apply(
            "Read all points in InfluxDB",
            InfluxDbIO.read()
                .withDataSourceConfiguration(
                    InfluxDbIO.DataSourceConfiguration.create(
                        options.getInfluxDBURL(),
                        options.getInfluxDBUserName(),
                        options.getInfluxDBPassword()))
                .withDatabase(options.getDatabaseName())
                .withMetrics(Arrays.asList("test_m", "test_m1"))
                .withSslInvalidHostNameAllowed(false)
                .withSslEnabled(false));
    PAssert.thatSingleton(values.apply("Count All", Count.globally()))
        .isEqualTo((long) numOfElementsToReadAndWrite * 2);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testWriteAndReadWithSingleMetricWithCustomRetentionPolicy() {
    final int noOfElementsToReadAndWrite = 2000;
    createRetentionPolicyInDB(options.getDatabaseName(), "test_retention");
    writePipeline
        .apply("Generate data", Create.of(GenerateData.getMetric(noOfElementsToReadAndWrite)))
        .apply(
            "Write data to InfluxDB",
            InfluxDbIO.write()
                .withConfiguration(
                    InfluxDbIO.DataSourceConfiguration.create(
                        options.getInfluxDBURL(),
                        options.getInfluxDBUserName(),
                        options.getInfluxDBPassword()))
                .withDatabase(options.getDatabaseName())
                .withRetentionPolicy("test_retention")
                .withSslInvalidHostNameAllowed(false)
                .withSslEnabled(false));
    writePipeline.run().waitUntilFinish();
    PCollection<String> values =
        readPipeline.apply(
            "Read all points in InfluxDB",
            InfluxDbIO.read()
                .withDataSourceConfiguration(
                    InfluxDbIO.DataSourceConfiguration.create(
                        options.getInfluxDBURL(),
                        options.getInfluxDBUserName(),
                        options.getInfluxDBPassword()))
                .withDatabase(options.getDatabaseName())
                .withRetentionPolicy("test_retention")
                .withMetrics(Collections.singletonList("test_m"))
                .withSslInvalidHostNameAllowed(false)
                .withSslEnabled(false));

    PAssert.thatSingleton(values.apply("Count All", Count.globally()))
        .isEqualTo((long) noOfElementsToReadAndWrite);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testWriteAndReadWithSQLForMultipleMetric() {
    final int noOfElementsToReadAndWrite = 2000;
    createRetentionPolicyInDB(options.getDatabaseName(), "test_rp");
    writePipeline
        .apply(
            "Generate data", Create.of(GenerateData.getMultipleMetric(noOfElementsToReadAndWrite)))
        .apply(
            "Write data to InfluxDB",
            InfluxDbIO.write()
                .withConfiguration(
                    InfluxDbIO.DataSourceConfiguration.create(
                        options.getInfluxDBURL(),
                        options.getInfluxDBUserName(),
                        options.getInfluxDBPassword()))
                .withDatabase(options.getDatabaseName())
                .withSslInvalidHostNameAllowed(false)
                .withRetentionPolicy("test_rp")
                .withSslEnabled(false));
    writePipeline.run().waitUntilFinish();
    PCollection<String> valuesForTestm =
        readPipeline.apply(
            "Read all points in InfluxDB For test_m metric",
            InfluxDbIO.read()
                .withDataSourceConfiguration(
                    InfluxDbIO.DataSourceConfiguration.create(
                        options.getInfluxDBURL(),
                        options.getInfluxDBUserName(),
                        options.getInfluxDBPassword()))
                .withDatabase(options.getDatabaseName())
                .withMetrics(Collections.singletonList("test_m"))
                .withRetentionPolicy("test_rp")
                .withSslInvalidHostNameAllowed(false)
                .withSslEnabled(false));
    PCollection<String> valuesForTestm1 =
        readPipeline.apply(
            "Read all points in InfluxDB For test_m1 metric",
            InfluxDbIO.read()
                .withDataSourceConfiguration(
                    InfluxDbIO.DataSourceConfiguration.create(
                        options.getInfluxDBURL(),
                        options.getInfluxDBUserName(),
                        options.getInfluxDBPassword()))
                .withDatabase(options.getDatabaseName())
                .withMetrics(Collections.singletonList("test_m1"))
                .withRetentionPolicy("test_rp")
                .withSslInvalidHostNameAllowed(false)
                .withSslEnabled(false));
    PAssert.thatSingleton(valuesForTestm1.apply("Count All For test_m1 metric", Count.globally()))
        .isEqualTo((long) noOfElementsToReadAndWrite);
    PAssert.thatSingleton(valuesForTestm.apply("Count All For test_m metric", Count.globally()))
        .isEqualTo((long) noOfElementsToReadAndWrite);
    readPipeline.run().waitUntilFinish();
  }
}
