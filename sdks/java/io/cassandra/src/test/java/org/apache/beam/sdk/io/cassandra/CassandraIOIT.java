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
import static org.junit.Assert.assertTrue;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
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
 * "--cassandraPort=7001"]'
 * }</pre>
 */
@RunWith(JUnit4.class)
public class CassandraIOIT {

  private static IOTestPipelineOptions options;

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void setup() throws Exception {
    PipelineOptionsFactory.register(IOTestPipelineOptions.class);
    options = TestPipeline.testingPipelineOptions()
        .as(IOTestPipelineOptions.class);
  }

  @Test
  public void testRead() throws Exception {
    PCollection<Scientist> output = pipeline.apply(CassandraIO.<Scientist>read()
        .withHosts(Collections.singletonList(options.getCassandraHost()))
        .withPort(options.getCassandraPort())
        .withKeyspace(CassandraTestDataSet.KEYSPACE)
        .withTable(CassandraTestDataSet.READ_TABLE_NAME)
        .withEntity(Scientist.class)
        .withCoder(SerializableCoder.of(Scientist.class)));

    PAssert.thatSingleton(output.apply("Count scientist", Count.<Scientist>globally()))
        .isEqualTo(1000L);

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
    PAssert.that(mapped.apply("Count occurences per scientist", Count.<String, Integer>perKey()))
        .satisfies(
            new SerializableFunction<Iterable<KV<String, Long>>, Void>() {
              @Override
              public Void apply(Iterable<KV<String, Long>> input) {
                for (KV<String, Long> element : input) {
                  assertEquals(element.getKey(), 1000 / 10, element.getValue().longValue());
                }
                return null;
              }
            }
        );

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testWrite() throws Exception {
    String tableName = CassandraTestDataSet.createWriteDataTable(options);

    ArrayList<Scientist> data = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      Scientist scientist = new Scientist();
      scientist.id = i;
      scientist.name = "Name " + i;
      data.add(scientist);
    }

    pipeline
        .apply(Create.of(data))
        .apply(CassandraIO.<Scientist>write()
            .withHosts(Collections.singletonList(options.getCassandraHost()))
            .withPort(options.getCassandraPort())
            .withKeyspace(CassandraTestDataSet.KEYSPACE)
            .withEntity(Scientist.class));

    pipeline.run().waitUntilFinish();

    Cluster cluster = CassandraTestDataSet.getCluster(options);
    Session session = cluster.connect();
    ResultSet result = session.execute("select id,name from " + CassandraTestDataSet.KEYSPACE
        + "." + tableName);
    List<Row> rows = result.all();
    assertEquals(1000, rows.size());
    for (Row row : rows) {
      assertTrue(row.getString("name").matches("Name.*"));
    }
  }

  /**
   * Simple Cassandra entity representing a scientist.
   */
  @Table(name = CassandraTestDataSet.READ_TABLE_NAME, keyspace = CassandraTestDataSet.KEYSPACE)
  public class Scientist implements Serializable {

    @Column(name = "id")
    public Integer id;

    @Column(name = "name")
    public String name;

    public String toString() {
      return id + ":" + name;
    }

  }

}
