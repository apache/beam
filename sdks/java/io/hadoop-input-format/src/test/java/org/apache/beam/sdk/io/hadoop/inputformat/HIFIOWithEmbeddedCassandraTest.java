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
package org.apache.beam.sdk.io.hadoop.inputformat;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;
import java.io.File;
import java.io.Serializable;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.io.common.NetworkTestHelper;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests to validate HadoopInputFormatIO for embedded Cassandra instance. */
@RunWith(JUnit4.class)
public class HIFIOWithEmbeddedCassandraTest implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final String CASSANDRA_KEYSPACE = "beamdb";
  private static final String CASSANDRA_HOST = "127.0.0.1";
  private static final String CASSANDRA_TABLE = "scientists";
  private static final String CASSANDRA_NATIVE_PORT_PROPERTY = "cassandra.input.native.port";
  private static final String CASSANDRA_THRIFT_PORT_PROPERTY = "cassandra.input.thrift.port";
  private static final String CASSANDRA_THRIFT_ADDRESS_PROPERTY = "cassandra.input.thrift.address";
  private static final String CASSANDRA_PARTITIONER_CLASS_PROPERTY =
      "cassandra.input.partitioner.class";
  private static final String CASSANDRA_PARTITIONER_CLASS_VALUE = "Murmur3Partitioner";
  private static final String CASSANDRA_KEYSPACE_PROPERTY = "cassandra.input.keyspace";
  private static final String CASSANDRA_COLUMNFAMILY_PROPERTY = "cassandra.input.columnfamily";
  private static int cassandraPort;
  private static int cassandraNativePort;
  private static transient Cluster cluster;
  private static transient Session session;
  private static final long TEST_DATA_ROW_COUNT = 10L;
  private static final EmbeddedCassandraService cassandra = new EmbeddedCassandraService();

  @Rule public final transient TestPipeline p = TestPipeline.create();

  /**
   * Test to read data from embedded Cassandra instance and verify whether data is read
   * successfully.
   *
   * @throws Exception
   */
  @Test
  public void testHIFReadForCassandra() {
    // Expected hashcode is evaluated during insertion time one time and hardcoded here.
    String expectedHashCode = "1b9780833cce000138b9afa25ba63486";
    Configuration conf = getConfiguration();
    PCollection<KV<Long, String>> cassandraData =
        p.apply(
            HadoopInputFormatIO.<Long, String>read()
                .withConfiguration(conf)
                .withValueTranslation(myValueTranslate));
    // Verify the count of data retrieved from Cassandra matches expected count.
    PAssert.thatSingleton(cassandraData.apply("Count", Count.globally()))
        .isEqualTo(TEST_DATA_ROW_COUNT);
    PCollection<String> textValues = cassandraData.apply(Values.create());
    // Verify the output values using checksum comparison.
    PCollection<String> consolidatedHashcode =
        textValues.apply(Combine.globally(new HashingFn()).withoutDefaults());
    PAssert.that(consolidatedHashcode).containsInAnyOrder(expectedHashCode);
    p.run().waitUntilFinish();
  }

  private final SimpleFunction<Row, String> myValueTranslate =
      new SimpleFunction<Row, String>() {
        @Override
        public String apply(Row input) {
          return input.getInt("id") + "|" + input.getString("scientist");
        }
      };

  /**
   * Test to read data from embedded Cassandra instance based on query and verify whether data is
   * read successfully.
   */
  @Test
  public void testHIFReadForCassandraQuery() {
    Long expectedCount = 1L;
    String expectedChecksum = "f11caabc7a9fc170e22b41218749166c";
    Configuration conf = getConfiguration();
    conf.set(
        "cassandra.input.cql",
        "select * from "
            + CASSANDRA_KEYSPACE
            + "."
            + CASSANDRA_TABLE
            + " where token(id) > ? and token(id) <= ? and scientist='Faraday1' allow filtering");
    PCollection<KV<Long, String>> cassandraData =
        p.apply(
            HadoopInputFormatIO.<Long, String>read()
                .withConfiguration(conf)
                .withValueTranslation(myValueTranslate));
    // Verify the count of data retrieved from Cassandra matches expected count.
    PAssert.thatSingleton(cassandraData.apply("Count", Count.globally())).isEqualTo(expectedCount);
    PCollection<String> textValues = cassandraData.apply(Values.create());
    // Verify the output values using checksum comparison.
    PCollection<String> consolidatedHashcode =
        textValues.apply(Combine.globally(new HashingFn()).withoutDefaults());
    PAssert.that(consolidatedHashcode).containsInAnyOrder(expectedChecksum);
    p.run().waitUntilFinish();
  }

  /**
   * Returns configuration of CqlInutFormat. Mandatory parameters required apart from inputformat
   * class name, key class, value class are thrift port, thrift address, partitioner class, keyspace
   * and columnfamily name
   */
  private Configuration getConfiguration() {
    Configuration conf = new Configuration();
    conf.set(CASSANDRA_NATIVE_PORT_PROPERTY, String.valueOf(cassandraNativePort));
    conf.set(CASSANDRA_THRIFT_PORT_PROPERTY, String.valueOf(cassandraPort));
    conf.set(CASSANDRA_THRIFT_ADDRESS_PROPERTY, CASSANDRA_HOST);
    conf.set(CASSANDRA_PARTITIONER_CLASS_PROPERTY, CASSANDRA_PARTITIONER_CLASS_VALUE);
    conf.set(CASSANDRA_KEYSPACE_PROPERTY, CASSANDRA_KEYSPACE);
    conf.set(CASSANDRA_COLUMNFAMILY_PROPERTY, CASSANDRA_TABLE);
    conf.setClass(
        "mapreduce.job.inputformat.class",
        org.apache.cassandra.hadoop.cql3.CqlInputFormat.class,
        InputFormat.class);
    conf.setClass("key.class", java.lang.Long.class, Object.class);
    conf.setClass("value.class", Row.class, Object.class);
    return conf;
  }

  private static void createCassandraData() {
    session.execute("DROP KEYSPACE IF EXISTS " + CASSANDRA_KEYSPACE);
    session.execute(
        "CREATE KEYSPACE "
            + CASSANDRA_KEYSPACE
            + " WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1};");
    session.execute("USE " + CASSANDRA_KEYSPACE);
    session.execute(
        "CREATE TABLE " + CASSANDRA_TABLE + "(id int, scientist text, PRIMARY KEY(id));");
    for (int i = 0; i < TEST_DATA_ROW_COUNT; i++) {
      session.execute(
          "INSERT INTO "
              + CASSANDRA_TABLE
              + "(id, scientist) values("
              + i
              + ", 'Faraday"
              + i
              + "');");
    }
  }

  @BeforeClass
  public static void startCassandra() throws Exception {
    cassandraPort = NetworkTestHelper.getAvailableLocalPort();
    cassandraNativePort = NetworkTestHelper.getAvailableLocalPort();
    replacePortsInConfFile();
    // Start the Embedded Cassandra Service
    cassandra.start();
    final SocketOptions socketOptions = new SocketOptions();
    // Setting this to 0 disables read timeouts.
    socketOptions.setReadTimeoutMillis(0);
    // This defaults to 5 s.  Increase to a minute.
    socketOptions.setConnectTimeoutMillis(60 * 1000);
    cluster =
        Cluster.builder()
            .addContactPoint(CASSANDRA_HOST)
            .withClusterName("beam")
            .withSocketOptions(socketOptions)
            .withPort(cassandraNativePort)
            .build();
    session = cluster.connect();
    createCassandraData();
  }

  private static void replacePortsInConfFile() throws Exception {
    URI uri = HIFIOWithEmbeddedCassandraTest.class.getResource("/cassandra.yaml").toURI();
    Path cassandraYamlPath = new File(uri).toPath();
    String content = new String(Files.readAllBytes(cassandraYamlPath), Charset.defaultCharset());
    content = content.replaceAll("9043", String.valueOf(cassandraNativePort));
    content = content.replaceAll("9161", String.valueOf(cassandraPort));
    Files.write(cassandraYamlPath, content.getBytes(Charset.defaultCharset()));
  }

  @AfterClass
  public static void stopEmbeddedCassandra() {
    session.close();
    cluster.close();
  }

  /** POJO class for scientist data. */
  @Table(name = CASSANDRA_TABLE, keyspace = CASSANDRA_KEYSPACE)
  public static class Scientist implements Serializable {
    private static final long serialVersionUID = 1L;

    @Column(name = "scientist")
    private String name;

    @Column(name = "id")
    private int id;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    @Override
    public String toString() {
      return id + ":" + name;
    }
  }
}
