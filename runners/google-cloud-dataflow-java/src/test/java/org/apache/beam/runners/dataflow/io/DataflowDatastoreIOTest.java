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
package org.apache.beam.runners.dataflow.io;

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;

import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;

import org.apache.beam.runners.dataflow.transforms.DataflowDisplayDataEvaluator;
import org.apache.beam.sdk.io.datastore.DatastoreIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayDataEvaluator;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;

import com.google.datastore.v1beta3.Entity;
import com.google.datastore.v1beta3.Query;

import org.junit.Test;

import java.util.Set;

/**
 * Unit tests for Dataflow usage of {@link DatastoreIO} transforms.
 */
public class DataflowDatastoreIOTest {
  @Test
  public void testSourcePrimitiveDisplayData() {
    DisplayDataEvaluator evaluator = DataflowDisplayDataEvaluator.create();
    PTransform<PBegin, ? extends POutput> read = DatastoreIO.v1beta3().read().withProjectId(
        "myProject").withQuery(Query.newBuilder().build());

    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveSourceTransforms(read);
    assertThat("DatastoreIO read should include the project in its primitive display data",
        displayData, hasItem(hasDisplayItem("projectId")));
  }

  @Test
  public void testSinkPrimitiveDisplayData() {
    DisplayDataEvaluator evaluator = DataflowDisplayDataEvaluator.create();
    PTransform<PCollection<Entity>, ?> write =
        DatastoreIO.v1beta3().write().withProjectId("myProject");

    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveTransforms(write);
    assertThat("DatastoreIO write should include the project in its primitive display data",
        displayData, hasItem(hasDisplayItem("projectId")));
  }
}
