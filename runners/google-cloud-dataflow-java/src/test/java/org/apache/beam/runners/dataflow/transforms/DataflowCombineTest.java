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
package org.apache.beam.runners.dataflow.transforms;

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;

import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineTest;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayDataEvaluator;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;

import org.junit.Test;

import java.util.Set;

/**
 * Unit tests for Dataflow usage of {@link Combine} transforms.
 */
public class DataflowCombineTest {
  @Test
  public void testCombinePerKeyPrimitiveDisplayData() {
    DisplayDataEvaluator evaluator = DataflowDisplayDataEvaluator.create();

    CombineTest.UniqueInts combineFn = new CombineTest.UniqueInts();
    PTransform<PCollection<KV<Integer, Integer>>, ? extends POutput> combine =
        Combine.perKey(combineFn);

    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveTransforms(combine,
        KvCoder.of(VarIntCoder.of(), VarIntCoder.of()));

    assertThat("Combine.perKey should include the combineFn in its primitive transform",
        displayData, hasItem(hasDisplayItem("combineFn", combineFn.getClass())));
  }
}
