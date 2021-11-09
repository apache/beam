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
package org.apache.beam.sdk.io.neo4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

@RunWith(JUnit4.class)
public class Neo4jIOIT {

  private static Network network;
  private static Neo4jContainer<?> neo4jContainer;

  @Rule public transient TestPipeline parameterizedReadPipeline = TestPipeline.create();
  @Rule public transient TestPipeline writeUnwindPipeline = TestPipeline.create();
  @Rule public transient TestPipeline largeWriteUnwindPipeline = TestPipeline.create();

  @BeforeClass
  public static void setup() throws Exception {
    // network sharing doesn't work with ClassRule
    network = Network.newNetwork();

    neo4jContainer =
        new Neo4jContainer<>(DockerImageName.parse("neo4j").withTag(Neo4jTestUtil.NEO4J_VERSION))
            .withStartupAttempts(1)
            .withExposedPorts(7687, 7474)
            .withNetwork(network)
            .withEnv(
                "NEO4J_AUTH", Neo4jTestUtil.NEO4J_USERNAME + "/" + Neo4jTestUtil.NEO4J_PASSWORD)
            .withEnv("NEO4J_dbms_default_listen_address", "0.0.0.0")
            .withNetworkAliases(Neo4jTestUtil.NEO4J_HOSTNAME)
            .withSharedMemorySize(256 * 1024 * 1024L); // 256MB

    // Start Neo4j
    neo4jContainer.start();

    // Start with an empty database to use for testing.
    // This prevents any possibility of some old data messing up the test results.
    // We add a unique constraint to see we're not trying to create nodes twice in the larger test
    // below
    //
    Neo4jTestUtil.executeOnNeo4j(
        "CREATE OR REPLACE DATABASE " + Neo4jTestUtil.NEO4J_DATABASE, false);
    Neo4jTestUtil.executeOnNeo4j(
        "CREATE CONSTRAINT something_id_unique ON (n:Something) ASSERT n.id IS UNIQUE", true);
  }

  @AfterClass
  public static void tearDown() {
    neo4jContainer.stop();
    neo4jContainer.close();

    try {
      network.close();
    } catch (Exception e) {
      // ignore
    }
  }

  @Test
  public void testParameterizedRead() throws Exception {
    PBegin begin = parameterizedReadPipeline.begin();
    PCollection<String> stringsCollections =
        begin.apply(Create.of(Arrays.asList("one", "two", "three")));

    final Schema outputSchema =
        Schema.of(
            Schema.Field.of("One", Schema.FieldType.INT32),
            Schema.Field.of("Str", Schema.FieldType.STRING));

    Neo4jIO.ParametersMapper<String> parametersMapper =
        (string, parametersMap) -> parametersMap.put("par1", string);

    Neo4jIO.RowMapper<Row> rowMapper =
        record -> {
          int one = record.get(0).asInt();
          String string = record.get(1).asString();
          return Row.withSchema(outputSchema).attachValues(one, string);
        };

    Neo4jIO.ReadAll<String, Row> read =
        Neo4jIO.<String, Row>readAll()
            .withCypher("RETURN 1, $par1")
            .withDatabase(Neo4jTestUtil.NEO4J_DATABASE)
            .withDriverConfiguration(Neo4jTestUtil.getDriverConfiguration())
            .withReadTransaction()
            .withFetchSize(5000)
            .withRowMapper(rowMapper)
            .withParametersMapper(parametersMapper)
            .withCoder(SerializableCoder.of(Row.class))
            .withCypherLogging();

    PCollection<Row> outputRows = stringsCollections.apply(read);

    PCollection<String> outputLines = outputRows.apply(ParDo.of(new RowToLineFn()));

    PAssert.that(outputLines).containsInAnyOrder("1,one", "1,two", "1,three");

    // Now run this pipeline
    //
    PipelineResult pipelineResult = parameterizedReadPipeline.run();

    Assert.assertEquals(PipelineResult.State.DONE, pipelineResult.getState());
  }

  @Test
  public void testWriteUnwind() throws Exception {
    PBegin begin = writeUnwindPipeline.begin();
    PCollection<String> stringsCollections =
        begin.apply(Create.of(Arrays.asList("one", "two", "three")));

    // Every row is represented by a Map<String, Object> in the parameters map.
    // We accumulate the rows and 'unwind' those to Neo4j for performance reasons.
    //
    Neo4jIO.ParametersMapper<String> parametersMapper =
        (string, rowMap) -> rowMap.put("name", string);

    Neo4jIO.WriteUnwind<String> read =
        Neo4jIO.<String>writeUnwind()
            .withDriverConfiguration(Neo4jTestUtil.getDriverConfiguration())
            .withDatabase(Neo4jTestUtil.NEO4J_DATABASE)
            .withBatchSize(5000)
            .withUnwindMapName("rows")
            .withCypher("UNWIND $rows AS row MERGE(n:Num { name : row.name })")
            .withParametersMapper(parametersMapper)
            .withCypherLogging();

    stringsCollections.apply(read);

    // Now run this pipeline
    //
    PipelineResult pipelineResult = writeUnwindPipeline.run();

    Assert.assertEquals(PipelineResult.State.DONE, pipelineResult.getState());

    // Connect back to the Instance and verify that we have 3 nodes
    //
    try (Driver driver = Neo4jTestUtil.getDriver()) {
      try (Session session = Neo4jTestUtil.getSession(driver, true)) {
        List<String> names =
            session.readTransaction(
                tx -> {
                  List<String> list = new ArrayList<>();
                  Result result = tx.run("MATCH(n:Num) RETURN n.name");
                  while (result.hasNext()) {
                    Record record = result.next();
                    list.add(record.get(0).asString());
                  }
                  return list;
                });
        Assert.assertEquals(3, names.size());
        Assert.assertTrue(names.contains("one"));
        Assert.assertTrue(names.contains("two"));
        Assert.assertTrue(names.contains("three"));
      }
    }
  }

  @Test
  public void testLargeWriteUnwind() throws Exception {
    PBegin begin = largeWriteUnwindPipeline.begin();

    // Create 1000 IDs
    List<Integer> idList = new ArrayList<>();
    for (int i = 5000; i < 6000; i++) {
      idList.add(i);
    }
    PCollection<Integer> idCollection = begin.apply(Create.of(idList));

    // Every row is represented by a Map<String, Object> in the parameters map.
    // We accumulate the rows and 'unwind' those to Neo4j for performance reasons.
    //
    Neo4jIO.ParametersMapper<Integer> parametersMapper = (id, rowMap) -> rowMap.put("id", id);

    // 1000 rows with a batch size of 123 should trigger most scenarios we can think of
    // We've put a unique constraint on Something.id
    //
    Neo4jIO.WriteUnwind<Integer> read =
        Neo4jIO.<Integer>writeUnwind()
            .withDriverConfiguration(Neo4jTestUtil.getDriverConfiguration())
            .withBatchSize(123)
            .withUnwindMapName("rows")
            .withCypher("UNWIND $rows AS row CREATE(n:Something { id : row.id })")
            .withDatabase(Neo4jTestUtil.NEO4J_DATABASE)
            .withParametersMapper(parametersMapper)
            .withCypherLogging();

    idCollection.apply(read);

    // Now run this pipeline
    //
    PipelineResult pipelineResult = largeWriteUnwindPipeline.run();

    Assert.assertEquals(PipelineResult.State.DONE, pipelineResult.getState());

    // Connect back to the Instance and verify that we have 1000 Something nodes
    //
    try (Driver driver = Neo4jTestUtil.getDriver()) {
      try (Session session = Neo4jTestUtil.getSession(driver, true)) {
        int[] values =
            session.readTransaction(
                tx -> {
                  int[] v = new int[4];
                  int nrRows = 0;
                  Result result =
                      tx.run("MATCH(n:Something) RETURN count(n), min(n.id), max(n.id)");
                  while (result.hasNext()) {
                    Record record = result.next();
                    v[0] = record.get(0).asInt();
                    v[1] = record.get(1).asInt();
                    v[2] = record.get(2).asInt();
                    v[3] = ++nrRows;
                  }
                  return v;
                });
        Assert.assertEquals(1000, values[0]);
        Assert.assertEquals(5000, values[1]);
        Assert.assertEquals(5999, values[2]);
        Assert.assertEquals(1, values[3]);
      }
    }
  }
}
