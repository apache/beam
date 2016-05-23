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

import org.apache.beam.runners.dataflow.DataflowPipelineRunner;
import org.apache.beam.runners.dataflow.transforms.DataflowDisplayDataEvaluator;
import org.apache.beam.sdk.io.PubsubIO;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayDataEvaluator;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Set;

/**
 * {@link DataflowPipelineRunner} specific tests for {@link PubsubIO} transforms.
 */
@RunWith(JUnit4.class)
public class DataflowPubsubIOTest {
  @Test
  public void testPrimitiveWriteDisplayData() {
    DisplayDataEvaluator evaluator = DataflowDisplayDataEvaluator.create();
    PubsubIO.Write.Bound<?> write = PubsubIO.Write.topic("projects/project/topics/topic");

    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveTransforms(write);
    assertThat("PubsubIO.Write should include the topic in its primitive display data",
               displayData, hasItem(hasDisplayItem("topic")));
  }

  @Test
  public void testPrimitiveReadDisplayData() {
    DisplayDataEvaluator evaluator = DataflowDisplayDataEvaluator.create();
    PubsubIO.Read.Bound<String> read =
        PubsubIO.Read.subscription("projects/project/subscriptions/subscription")
                     .maxNumRecords(1);

    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveTransforms(read);
    assertThat("PubsubIO.Read should include the subscription in its primitive display data",
               displayData, hasItem(hasDisplayItem("subscription")));
  }
}
