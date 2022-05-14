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
package org.apache.beam.sdk.extensions.python;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesPythonExpansionService;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.util.PythonCallableSource;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p43p2.io.grpc.ConnectivityState;
import org.apache.beam.vendor.grpc.v1p43p2.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p43p2.io.grpc.ManagedChannelBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DataframeTransformTest {
  @Rule public transient TestPipeline testPipeline = TestPipeline.create();
  private PipelineResult pipelineResult;
  private static String expansionAddr;

  @BeforeClass
  public static void setUpClass() {
    expansionAddr =
        String.format("localhost:%s", Integer.valueOf(System.getProperty("expansionPort")));
  }

  @Before
  public void setUp() {
    waitForReady();
  }

  @After
  public void tearDown() {
    pipelineResult = testPipeline.run();
    pipelineResult.waitUntilFinish();
    assertThat(pipelineResult.getState(), equalTo(PipelineResult.State.DONE));
  }

  private void waitForReady() {
    try {
      ManagedChannel channel = ManagedChannelBuilder.forTarget(expansionAddr).build();
      ConnectivityState state = channel.getState(true);
      for (int retry = 0; retry < 30 && state != ConnectivityState.READY; retry++) {
        Thread.sleep(500);
        state = channel.getState(true);
      }
      channel.shutdownNow();
    } catch (InterruptedException e) {
      throw new RuntimeException("interrupted.");
    }
  }

  @Test
  @Category({ValidatesRunner.class, UsesPythonExpansionService.class})
  public void testDataframeSum() {
    Schema schema =
        Schema.of(
            Schema.Field.of("a", Schema.FieldType.INT64),
            Schema.Field.of("b", Schema.FieldType.INT32));
    Row foo1 = Row.withSchema(schema).withFieldValue("a", 100L).withFieldValue("b", 1).build();
    Row foo2 = Row.withSchema(schema).withFieldValue("a", 100L).withFieldValue("b", 2).build();
    Row foo3 = Row.withSchema(schema).withFieldValue("a", 100L).withFieldValue("b", 3).build();
    Row bar4 = Row.withSchema(schema).withFieldValue("a", 200L).withFieldValue("b", 4).build();
    PCollection<Row> col =
        testPipeline
            .apply(Create.of(foo1, foo2, bar4))
            .setRowSchema(schema)
            .apply(
                PythonExternalTransform.<PCollection<Row>, PCollection<Row>>from(
                        "apache_beam.dataframe.transforms.DataframeTransform", expansionAddr)
                    .withKwarg("func", PythonCallableSource.of("lambda df: df.groupby('a').sum()"))
                    .withKwarg("include_indexes", true));
    PAssert.that(col).containsInAnyOrder(foo3, bar4);
  }
}
