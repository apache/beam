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
package org.apache.beam.sdk.io.cassandra;

import static org.junit.Assert.assertEquals;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SerializableMatcher;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * A test of {@link CassandraIO} on a concrete and independent Cassandra instance.
 *
 * <p>This test requires a running Cassandra instance, and the test dataset must exists.
 *
 * <p>You can run this test directly using Maven with:
 *
 * <pre>{@code
 * mvn -e -Pio-it verify -pl sdks/java/io/cassandra -DintegrationTestPipelineOptions='[
 * "--cassandraHost=1.2.3.4",
 * "--cassandraPort=9042"]'
 * }</pre>
 */
@RunWith(JUnit4.class)
public class CassandraIOIT implements Serializable {

  private static IOTestPipelineOptions options;

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void setup() throws Exception {
    PipelineOptionsFactory.register(IOTestPipelineOptions.class);
    options = TestPipeline.testingPipelineOptions()
        .as(IOTestPipelineOptions.class);
  }

  @AfterClass
  public static void tearDown() {
    // cleanup the write table
    CassandraTestDataSet.cleanUpDataTable(options);
  }

  @Test
  public void testRead() throws Exception {
    PCollection<Scientist> output = pipeline.apply(CassandraIO.<Scientist>read()
        .withHosts(Collections.singletonList(options.getCassandraHost()))
        .withPort(options.getCassandraPort())
        .withKeyspace(CassandraTestDataSet.KEYSPACE)
        .withTable(CassandraTestDataSet.TABLE_READ_NAME)
        .withEntity(Scientist.class)
        .withCoder(SerializableCoder.of(Scientist.class)));

    PAssert.thatSingleton(output.apply("Count scientist", Count.globally())).isEqualTo(1000L);

    PCollection<KV<String, Integer>> mapped =
        output.apply(
            MapElements.via(
                new SimpleFunction<Scientist, KV<String, Integer>>() {
                  public KV<String, Integer> apply(Scientist scientist) {
                    KV<String, Integer> kv = KV.of(scientist.name, scientist.id);
                    return kv;
                  }
                }
            )
        );
    PAssert.that(mapped.apply("Count occurrences per scientist", Count.perKey()))
        .satisfies(
            input -> {
              for (KV<String, Long> element : input) {
                assertEquals(element.getKey(), 1000 / 10, element.getValue().longValue());
              }
              return null;
            });

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testWrite() throws Exception {
    IOTestPipelineOptions options =
        TestPipeline.testingPipelineOptions().as(IOTestPipelineOptions.class);

    options.setOnSuccessMatcher(
        new CassandraMatcher(
            CassandraTestDataSet.getCluster(options),
            CassandraTestDataSet.TABLE_WRITE_NAME));

    ArrayList<ScientistForWrite> data = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      ScientistForWrite scientist = new ScientistForWrite();
      scientist.id = i;
      scientist.name = "Name " + i;
      data.add(scientist);
    }

    pipeline
        .apply(Create.of(data))
        .apply(CassandraIO.<ScientistForWrite>write()
            .withHosts(Collections.singletonList(options.getCassandraHost()))
            .withPort(options.getCassandraPort())
            .withKeyspace(CassandraTestDataSet.KEYSPACE)
            .withEntity(ScientistForWrite.class));

    pipeline.run().waitUntilFinish();
  }

  /**
   * Simple matcher.
   */
  public class CassandraMatcher extends TypeSafeMatcher<PipelineResult>
      implements SerializableMatcher<PipelineResult> {

    private String tableName;
    private Cluster cluster;

    public CassandraMatcher(Cluster cluster, String tableName) {
      this.cluster = cluster;
      this.tableName = tableName;
    }

    @Override
    protected boolean matchesSafely(PipelineResult pipelineResult) {
      pipelineResult.waitUntilFinish();
      Session session = cluster.connect();
      ResultSet result = session.execute("select id,name from " + CassandraTestDataSet.KEYSPACE
          + "." + tableName);
      List<Row> rows = result.all();
      if (rows.size() != 1000) {
        return false;
      }
      for (Row row : rows) {
        if (!row.getString("name").matches("Name.*")) {
          return false;
        }
      }
      return true;
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("Expected Cassandra record pattern is (Name.*)");
    }
  }

  /**
   * Simple Cassandra entity representing a scientist. Used for read test.
   */
  @Table(name = CassandraTestDataSet.TABLE_READ_NAME, keyspace = CassandraTestDataSet.KEYSPACE)
  public static class Scientist implements Serializable {

    @PartitionKey
    @Column(name = "id")
    private final int id;

    @Column(name = "name")
    private final String name;

    public Scientist() {
      this(0, "");
    }

    public Scientist(int id) {
      this(0, "");
    }

    public Scientist(int id, String name) {
      this.id = id;
      this.name = name;
    }

    public int getId() {
      return id;
    }

    public String getName() {
      return name;
    }
  }

  /**
   * Simple Cassandra entity representing a scientist, used for write test.
   */
  @Table(name = CassandraTestDataSet.TABLE_WRITE_NAME, keyspace = CassandraTestDataSet.KEYSPACE)
  public class ScientistForWrite implements Serializable {

    @PartitionKey
    @Column(name = "id")
    public Integer id;

    @Column(name = "name")
    public String name;

    public String toString() {
      return id + ":" + name;
    }

  }

}
