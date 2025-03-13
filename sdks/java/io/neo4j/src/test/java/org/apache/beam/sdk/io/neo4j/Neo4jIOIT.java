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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
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
import org.neo4j.driver.SessionConfig;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.utility.DockerImageName;

@RunWith(JUnit4.class)
public class Neo4jIOIT {

  private static Neo4jContainer<?> neo4jContainer;
  private static String containerHostname;
  private static int containerPort;

  @Rule public transient TestPipeline parameterizedReadPipeline = TestPipeline.create();
  @Rule public transient TestPipeline writeUnwindPipeline = TestPipeline.create();
  @Rule public transient TestPipeline largeWriteUnwindPipeline = TestPipeline.create();

  @BeforeClass
  public static void setup() throws Exception {
    neo4jContainer =
        new Neo4jContainer<>(DockerImageName.parse("neo4j").withTag(Neo4jTestUtil.NEO4J_VERSION))
            .withStartupAttempts(1)
            .withAdminPassword(Neo4jTestUtil.NEO4J_PASSWORD)
            .withEnv("dbms_default_listen_address", "0.0.0.0")
            .withNetworkAliases(Neo4jTestUtil.NEO4J_NETWORK_ALIAS)
            .withSharedMemorySize(256 * 1024 * 1024L); // 256MB

    // Start Neo4j
    neo4jContainer.start();

    // Start with an empty database to use for testing.
    // This prevents any possibility of some old data messing up the test results.
    // We add a unique constraint to see we're not trying to create nodes twice in the larger test
    // below
    //
    containerHostname = neo4jContainer.getContainerIpAddress();
    containerPort = neo4jContainer.getMappedPort(7687);

    Neo4jTestUtil.executeOnNeo4j(
        containerHostname,
        containerPort,
        "CREATE CONSTRAINT something_id_unique FOR (n:Something) REQUIRE n.id IS UNIQUE",
        true);
  }

  @AfterClass
  public static void tearDown() {
    neo4jContainer.stop();
    neo4jContainer.close();
  }

  private static class ParameterizedReadRowToLineFn extends DoFn<Row, String> {
    @DoFn.ProcessElement
    public void processElement(ProcessContext context) {
      Row row = context.element();
      assert row != null;
      int one = row.getInt32(0);
      String string = row.getString(1);
      context.output(one + "," + string);
    }
  }

  @Test
  public void testParameterizedRead() throws Exception {
    PCollection<String> stringsCollections =
        parameterizedReadPipeline.apply(Create.of(Arrays.asList("one", "two", "three")));

    final Schema outputSchema =
        Schema.of(
            Schema.Field.of("One", Schema.FieldType.INT32),
            Schema.Field.of("Str", Schema.FieldType.STRING));

    SerializableFunction<String, Map<String, Object>> parametersFunction =
        string -> Collections.singletonMap("par1", string);

    Neo4jIO.RowMapper<Row> rowMapper =
        record -> {
          int one = record.get(0).asInt();
          String string = record.get(1).asString();
          return Row.withSchema(outputSchema).attachValues(one, string);
        };

    Neo4jIO.ReadAll<String, Row> read =
        Neo4jIO.<String, Row>readAll()
            .withCypher("RETURN 1, $par1")
            .withDriverConfiguration(
                Neo4jTestUtil.getDriverConfiguration(containerHostname, containerPort))
            .withSessionConfig(SessionConfig.forDatabase(Neo4jTestUtil.NEO4J_DATABASE))
            .withRowMapper(rowMapper)
            .withParametersFunction(parametersFunction)
            .withCoder(SerializableCoder.of(Row.class))
            .withCypherLogging();

    PCollection<Row> outputRows = stringsCollections.apply(read);

    PCollection<String> outputLines =
        outputRows.apply(ParDo.of(new ParameterizedReadRowToLineFn()));

    PAssert.that(outputLines).containsInAnyOrder("1,one", "1,two", "1,three");

    // Now run this pipeline
    //
    PipelineResult pipelineResult = parameterizedReadPipeline.run();

    Assert.assertEquals(PipelineResult.State.DONE, pipelineResult.getState());
  }

  @Test
  public void testWriteUnwind() throws Exception {
    PCollection<String> stringsCollections =
        writeUnwindPipeline.apply(Create.of(Arrays.asList("one", "two", "three")));

    // Every row is represented by a Map<String, Object> in the parameters map.
    // We accumulate the rows and 'unwind' those to Neo4j for performance reasons.
    //
    SerializableFunction<String, Map<String, Object>> parametersMapper =
        name -> Collections.singletonMap("name", name);

    Neo4jIO.WriteUnwind<String> read =
        Neo4jIO.<String>writeUnwind()
            .withDriverConfiguration(
                Neo4jTestUtil.getDriverConfiguration(containerHostname, containerPort))
            .withSessionConfig(SessionConfig.forDatabase(Neo4jTestUtil.NEO4J_DATABASE))
            .withBatchSize(5000)
            .withUnwindMapName("rows")
            .withCypher("UNWIND $rows AS row MERGE(n:Num { name : row.name })")
            .withParametersFunction(parametersMapper)
            .withCypherLogging();

    stringsCollections.apply(read);

    // Now run this pipeline
    //
    PipelineResult pipelineResult = writeUnwindPipeline.run();

    Assert.assertEquals(PipelineResult.State.DONE, pipelineResult.getState());

    // Connect back to the Instance and verify that we have 3 nodes
    //
    try (Driver driver = Neo4jTestUtil.getDriver(containerHostname, containerPort)) {
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

        assertThat(names, containsInAnyOrder("one", "two", "three"));
      }
    }
  }

  @Test
  public void testLargeWriteUnwind() throws Exception {
    final int startId = 5000;
    final int endId = 6000;
    // Create 1000 IDs
    List<Integer> idList = new ArrayList<>();
    for (int id = startId; id < endId; id++) {
      idList.add(id);
    }
    PCollection<Integer> idCollection = largeWriteUnwindPipeline.apply(Create.of(idList));

    // Every row is represented by a Map<String, Object> in the parameters map.
    // We accumulate the rows and 'unwind' those to Neo4j for performance reasons.
    //
    SerializableFunction<Integer, Map<String, Object>> parametersFunction =
        id -> ImmutableMap.of("id", id, "name", "Casters", "firstName", "Matt");

    // 1000 rows with a batch size of 123 should trigger most scenarios we can think of
    // We've put a unique constraint on Something.id
    //
    Neo4jIO.WriteUnwind<Integer> read =
        Neo4jIO.<Integer>writeUnwind()
            .withDriverConfiguration(
                Neo4jTestUtil.getDriverConfiguration(containerHostname, containerPort))
            .withSessionConfig(SessionConfig.forDatabase(Neo4jTestUtil.NEO4J_DATABASE))
            .withBatchSize(123)
            .withUnwindMapName("rows")
            .withCypher("UNWIND $rows AS row CREATE(n:Something { id : row.id })")
            .withParametersFunction(parametersFunction)
            .withCypherLogging();

    idCollection.apply(read);

    // Now run this pipeline
    //
    PipelineResult pipelineResult = largeWriteUnwindPipeline.run();

    Assert.assertEquals(PipelineResult.State.DONE, pipelineResult.getState());

    // Connect back to the Instance and verify that we have 1000 Something nodes
    //
    try (Driver driver = Neo4jTestUtil.getDriver(containerHostname, containerPort)) {
      try (Session session = Neo4jTestUtil.getSession(driver, true)) {
        List<Integer> values =
            session.readTransaction(
                tx -> {
                  List<Integer> v = null;
                  int nrRows = 0;
                  Result result =
                      tx.run("MATCH(n:Something) RETURN count(n), min(n.id), max(n.id)");
                  while (result.hasNext()) {
                    Record record = result.next();
                    v =
                        Arrays.asList(
                            record.get(0).asInt(),
                            record.get(1).asInt(),
                            record.get(2).asInt(),
                            ++nrRows);
                  }
                  return v;
                });

        Assert.assertNotNull(values);
        assertThat(values, contains(endId - startId, startId, endId - 1, 1));
      }
    }
  }
}
