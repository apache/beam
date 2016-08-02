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

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;

import static com.google.datastore.v1beta3.client.DatastoreHelper.makeKey;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.io.gcp.datastore.V1Beta3.DatastoreWriter;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.RunnableOnService;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayDataEvaluator;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;

import com.google.common.collect.Lists;
import com.google.datastore.v1beta3.Entity;
import com.google.datastore.v1beta3.Key;
import com.google.datastore.v1beta3.Query;
import com.google.datastore.v1beta3.client.Datastore;
import com.google.protobuf.Int32Value;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Tests for {@link V1Beta3}.
 */
@RunWith(JUnit4.class)
public class V1Beta3Test {
  private static final String PROJECT_ID = "testProject";
  private static final String NAMESPACE = "testNamespace";
  private static final String KIND = "testKind";
  private static final Query QUERY;
  static {
    Query.Builder q = Query.newBuilder();
    q.addKindBuilder().setName(KIND);
    QUERY = q.build();
  }
  private V1Beta3.Read initialRead;

  @Mock
  Datastore mockDatastore;

  @Rule
  public final ExpectedException thrown = ExpectedException.none();

  @Rule public final ExpectedLogs logged = ExpectedLogs.none(V1Beta3Source.class);

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    initialRead = DatastoreIO.v1beta3().read()
        .withProjectId(PROJECT_ID).withQuery(QUERY).withNamespace(NAMESPACE);
  }

  @Test
  public void testBuildRead() throws Exception {
    V1Beta3.Read read = DatastoreIO.v1beta3().read()
        .withProjectId(PROJECT_ID).withQuery(QUERY).withNamespace(NAMESPACE);
    assertEquals(QUERY, read.getQuery());
    assertEquals(PROJECT_ID, read.getProjectId());
    assertEquals(NAMESPACE, read.getNamespace());
  }

  /**
   * {@link #testBuildRead} but constructed in a different order.
   */
  @Test
  public void testBuildReadAlt() throws Exception {
    V1Beta3.Read read =  DatastoreIO.v1beta3().read()
        .withProjectId(PROJECT_ID).withNamespace(NAMESPACE).withQuery(QUERY);
    assertEquals(QUERY, read.getQuery());
    assertEquals(PROJECT_ID, read.getProjectId());
    assertEquals(NAMESPACE, read.getNamespace());
  }

  @Test
  public void testReadValidationFailsProject() throws Exception {
    V1Beta3.Read read =  DatastoreIO.v1beta3().read().withQuery(QUERY);
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("project");
    read.validate(null);
  }

  @Test
  public void testReadValidationFailsQuery() throws Exception {
    V1Beta3.Read read =  DatastoreIO.v1beta3().read().withProjectId(PROJECT_ID);
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("query");
    read.validate(null);
  }

  @Test
  public void testReadValidationFailsQueryLimitZero() throws Exception {
    Query invalidLimit = Query.newBuilder().setLimit(Int32Value.newBuilder().setValue(0)).build();
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid query limit 0: must be positive");

    DatastoreIO.v1beta3().read().withQuery(invalidLimit);
  }

  @Test
  public void testReadValidationFailsQueryLimitNegative() throws Exception {
    Query invalidLimit = Query.newBuilder().setLimit(Int32Value.newBuilder().setValue(-5)).build();
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid query limit -5: must be positive");

    DatastoreIO.v1beta3().read().withQuery(invalidLimit);
  }

  @Test
  public void testReadValidationSucceedsNamespace() throws Exception {
    V1Beta3.Read read =  DatastoreIO.v1beta3().read().withProjectId(PROJECT_ID).withQuery(QUERY);
    /* Should succeed, as a null namespace is fine. */
    read.validate(null);
  }

  @Test
  public void testReadDisplayData() {
    V1Beta3.Read read =  DatastoreIO.v1beta3().read()
      .withProjectId(PROJECT_ID)
      .withQuery(QUERY)
      .withNamespace(NAMESPACE);

    DisplayData displayData = DisplayData.from(read);

    assertThat(displayData, hasDisplayItem("projectId", PROJECT_ID));
    assertThat(displayData, hasDisplayItem("query", QUERY.toString()));
    assertThat(displayData, hasDisplayItem("namespace", NAMESPACE));
  }

  @Test
  @Category(RunnableOnService.class)
  public void testSourcePrimitiveDisplayData() {
    DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();
    PTransform<PBegin, ? extends POutput> read = DatastoreIO.v1beta3().read().withProjectId(
        "myProject").withQuery(Query.newBuilder().build());

    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveSourceTransforms(read);
    assertThat("DatastoreIO read should include the project in its primitive display data",
        displayData, hasItem(hasDisplayItem("projectId")));
  }

  @Test
  public void testWriteDoesNotAllowNullProject() throws Exception {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("projectId");

    DatastoreIO.v1beta3().write().withProjectId(null);
  }

  @Test
  public void testWriteValidationFailsWithNoProject() throws Exception {
    V1Beta3.Write write =  DatastoreIO.v1beta3().write();

    thrown.expect(NullPointerException.class);
    thrown.expectMessage("projectId");

    write.validate(null);
  }

  @Test
  public void testSinkValidationSucceedsWithProject() throws Exception {
    V1Beta3.Write write =  DatastoreIO.v1beta3().write().withProjectId(PROJECT_ID);
    write.validate(null);
  }

  @Test
  public void testWriteDisplayData() {
    V1Beta3.Write write =  DatastoreIO.v1beta3().write()
        .withProjectId(PROJECT_ID);

    DisplayData displayData = DisplayData.from(write);

    assertThat(displayData, hasDisplayItem("projectId", PROJECT_ID));
  }

  @Test
  @Category(RunnableOnService.class)
  public void testSinkPrimitiveDisplayData() {
    DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();
    PTransform<PCollection<Entity>, ?> write =
        DatastoreIO.v1beta3().write().withProjectId("myProject");

    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveTransforms(write);
    assertThat("DatastoreIO write should include the project in its primitive display data",
        displayData, hasItem(hasDisplayItem("projectId")));
  }

  /**
   * Test building a Write using builder methods.
   */
  @Test
  public void testBuildWrite() throws Exception {
    V1Beta3.Write write =  DatastoreIO.v1beta3().write().withProjectId(PROJECT_ID);
    assertEquals(PROJECT_ID, write.getProjectId());
  }

  /**
   * Test the detection of complete and incomplete keys.
   */
  @Test
  public void testHasNameOrId() {
    Key key;
    // Complete with name, no ancestor
    key = makeKey("bird", "finch").build();
    assertTrue(DatastoreWriter.isValidKey(key));

    // Complete with id, no ancestor
    key = makeKey("bird", 123).build();
    assertTrue(DatastoreWriter.isValidKey(key));

    // Incomplete, no ancestor
    key = makeKey("bird").build();
    assertFalse(DatastoreWriter.isValidKey(key));

    // Complete with name and ancestor
    key = makeKey("bird", "owl").build();
    key = makeKey(key, "bird", "horned").build();
    assertTrue(DatastoreWriter.isValidKey(key));

    // Complete with id and ancestor
    key = makeKey("bird", "owl").build();
    key = makeKey(key, "bird", 123).build();
    assertTrue(DatastoreWriter.isValidKey(key));

    // Incomplete with ancestor
    key = makeKey("bird", "owl").build();
    key = makeKey(key, "bird").build();
    assertFalse(DatastoreWriter.isValidKey(key));

    key = makeKey().build();
    assertFalse(DatastoreWriter.isValidKey(key));
  }

  /**
   * Test that entities with incomplete keys cannot be updated.
   */
  @Test
  public void testAddEntitiesWithIncompleteKeys() throws Exception {
    Key key = makeKey("bird").build();
    Entity entity = Entity.newBuilder().setKey(key).build();
    DatastoreWriter writer = new DatastoreWriter(null, mockDatastore);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Entities to be written to the Datastore must have complete keys");

    writer.write(entity);
  }

  /**
   * Test that entities are added to the batch to update.
   */
  @Test
  public void testAddingEntities() throws Exception {
    List<Entity> expected = Lists.newArrayList(
        Entity.newBuilder().setKey(makeKey("bird", "jay").build()).build(),
        Entity.newBuilder().setKey(makeKey("bird", "condor").build()).build(),
        Entity.newBuilder().setKey(makeKey("bird", "robin").build()).build());

    List<Entity> allEntities = Lists.newArrayList(expected);
    Collections.shuffle(allEntities);

    DatastoreWriter writer = new DatastoreWriter(null, mockDatastore);
    writer.open("test_id");
    for (Entity entity : allEntities) {
      writer.write(entity);
    }

    assertEquals(expected.size(), writer.entities.size());
    assertThat(writer.entities, containsInAnyOrder(expected.toArray()));
  }

  /** Datastore batch API limit in number of records per query. */
  private static final int DATASTORE_QUERY_BATCH_LIMIT = 500;
}
