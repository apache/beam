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
package org.apache.beam.sdk.io.gcp.datastore;

import static org.apache.beam.sdk.io.gcp.datastore.V1TestUtil.deleteAllEntities;
import static org.apache.beam.sdk.io.gcp.datastore.V1TestUtil.getDatastore;
import static org.apache.beam.sdk.io.gcp.datastore.V1TestUtil.makeAncestorKey;
import static org.apache.beam.sdk.io.gcp.datastore.V1TestUtil.makeEntity;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.client.Datastore;
import java.util.UUID;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.datastore.V1TestUtil.UpsertMutationBuilder;
import org.apache.beam.sdk.io.gcp.datastore.V1TestUtil.V1TestWriter;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.PCollection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** End-to-end tests for Datastore DatastoreV1.Read. */
@RunWith(JUnit4.class)
public class V1ReadIT {
  private V1TestOptions options;
  private String project;
  private String ancestor;
  private final long numEntities = 1000;

  @Before
  public void setup() throws Exception {
    PipelineOptionsFactory.register(V1TestOptions.class);
    options = TestPipeline.testingPipelineOptions().as(V1TestOptions.class);
    project = TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
    ancestor = UUID.randomUUID().toString();
    // Create entities and write them to datastore
    writeEntitiesToDatastore(options, project, ancestor, numEntities);
  }

  @After
  public void tearDown() throws Exception {
    deleteAllEntities(options, project, ancestor);
  }

  /**
   * An end-to-end test for {@link DatastoreV1.Read#withQuery(Query)}
   *
   * <p>Write some test entities to datastore and then run a pipeline that reads and counts the
   * total number of entities. Verify that the count matches the number of entities written.
   */
  @Test
  public void testE2EV1Read() throws Exception {
    // Read from datastore
    Query query =
        V1TestUtil.makeAncestorKindQuery(options.getKind(), options.getNamespace(), ancestor);

    DatastoreV1.Read read =
        DatastoreIO.v1()
            .read()
            .withProjectId(project)
            .withQuery(query)
            .withNamespace(options.getNamespace());

    // Count the total number of entities
    Pipeline p = Pipeline.create(options);
    PCollection<Long> count = p.apply(read).apply(Count.globally());

    PAssert.thatSingleton(count).isEqualTo(numEntities);
    p.run();
  }

  @Test
  public void testE2EV1ReadWithGQLQueryWithNoLimit() throws Exception {
    testE2EV1ReadWithGQLQuery(0);
  }

  @Test
  public void testE2EV1ReadWithGQLQueryWithLimit() throws Exception {
    testE2EV1ReadWithGQLQuery(99);
  }

  /**
   * An end-to-end test for {@link DatastoreV1.Read#withLiteralGqlQuery(String)}.
   *
   * <p>Write some test entities to datastore and then run a pipeline that reads and counts the
   * total number of entities. Verify that the count matches the number of entities written.
   */
  private void testE2EV1ReadWithGQLQuery(long limit) throws Exception {
    String gqlQuery =
        String.format(
            "SELECT * from %s WHERE __key__ HAS ANCESTOR KEY(%s, '%s')",
            options.getKind(), options.getKind(), ancestor);

    long expectedNumEntities = numEntities;
    if (limit > 0) {
      gqlQuery = String.format("%s LIMIT %d", gqlQuery, limit);
      expectedNumEntities = limit;
    }

    DatastoreV1.Read read =
        DatastoreIO.v1()
            .read()
            .withProjectId(project)
            .withLiteralGqlQuery(gqlQuery)
            .withNamespace(options.getNamespace());

    // Count the total number of entities
    Pipeline p = Pipeline.create(options);
    PCollection<Long> count = p.apply(read).apply(Count.globally());

    PAssert.thatSingleton(count).isEqualTo(expectedNumEntities);
    p.run();
  }

  // Creates entities and write them to datastore
  private static void writeEntitiesToDatastore(
      V1TestOptions options, String project, String ancestor, long numEntities) throws Exception {
    Datastore datastore = getDatastore(options, project);
    // Write test entities to datastore
    V1TestWriter writer = new V1TestWriter(datastore, new UpsertMutationBuilder());
    Key ancestorKey = makeAncestorKey(options.getNamespace(), options.getKind(), ancestor);

    for (long i = 0; i < numEntities; i++) {
      Entity entity = makeEntity(i, ancestorKey, options.getKind(), options.getNamespace(), 0);
      writer.write(entity);
    }
    writer.close();
  }
}
