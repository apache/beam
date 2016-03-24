/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.values;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.RunnableOnService;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.values.PCollection.IsBounded;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/** Unit tests for {@link PCollectionTuple}. */
@RunWith(JUnit4.class)
public final class PCollectionTupleTest implements Serializable {
  @Test
  public void testOfThenHas() {
    Pipeline pipeline = TestPipeline.create();
    PCollection<Object> pCollection = PCollection.createPrimitiveOutputInternal(
        pipeline, WindowingStrategy.globalDefault(), IsBounded.BOUNDED);
    TupleTag<Object> tag = new TupleTag<>();

    assertTrue(PCollectionTuple.of(tag, pCollection).has(tag));
  }

  @Test
  public void testEmpty() {
    Pipeline pipeline = TestPipeline.create();
    TupleTag<Object> tag = new TupleTag<>();
    assertFalse(PCollectionTuple.empty(pipeline).has(tag));
  }

  @Test
  @Category(RunnableOnService.class)
  public void testComposePCollectionTuple() {
    Pipeline pipeline = TestPipeline.create();

    List<Integer> inputs = Arrays.asList(3, -42, 666);

    TupleTag<Integer> mainOutputTag = new TupleTag<Integer>("main") {};
    TupleTag<Integer> emptyOutputTag = new TupleTag<Integer>("empty") {};
    final TupleTag<Integer> sideOutputTag = new TupleTag<Integer>("side") {};

    PCollection<Integer> mainInput = pipeline
        .apply(Create.of(inputs));

    PCollectionTuple outputs = mainInput.apply(ParDo
        .of(new DoFn<Integer, Integer>() {
          @Override
          public void processElement(ProcessContext c) {
            c.sideOutput(sideOutputTag, c.element());
          }})
        .withOutputTags(emptyOutputTag, TupleTagList.of(sideOutputTag)));
    assertNotNull("outputs.getPipeline()", outputs.getPipeline());
    outputs = outputs.and(mainOutputTag, mainInput);

    DataflowAssert.that(outputs.get(mainOutputTag)).containsInAnyOrder(inputs);
    DataflowAssert.that(outputs.get(sideOutputTag)).containsInAnyOrder(inputs);
    DataflowAssert.that(outputs.get(emptyOutputTag)).empty();

    pipeline.run();
  }

}
