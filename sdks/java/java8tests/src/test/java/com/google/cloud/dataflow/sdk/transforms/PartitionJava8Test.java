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

package com.google.cloud.dataflow.sdk.transforms;

import static org.junit.Assert.assertEquals;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.CannotProvideCoderException;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.values.PCollectionList;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;

/**
 * Java 8 Tests for {@link Filter}.
 */
@RunWith(JUnit4.class)
@SuppressWarnings("serial")
public class PartitionJava8Test implements Serializable {

  @Rule
  public transient ExpectedException thrown = ExpectedException.none();

  @Test
  public void testModPartition() {
    Pipeline pipeline = TestPipeline.create();

    PCollectionList<Integer> outputs = pipeline
        .apply(Create.of(1, 2, 4, 5))
        .apply(Partition.of(3, (element, numPartitions) -> element % numPartitions));
    assertEquals(3, outputs.size());
    DataflowAssert.that(outputs.get(0)).empty();
    DataflowAssert.that(outputs.get(1)).containsInAnyOrder(1, 4);
    DataflowAssert.that(outputs.get(2)).containsInAnyOrder(2, 5);
    pipeline.run();
  }

  /**
   * Confirms that in Java 8 style, where a lambda results in a rawtype, the output type token is
   * not useful. If this test ever fails there may be simplifications available to us.
   */
  @Test
  public void testPartitionFnOutputTypeDescriptorRaw() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    PCollectionList<String> output = pipeline
        .apply(Create.of("hello"))
        .apply(Partition.of(1, (element, numPartitions) -> 0));

    thrown.expect(CannotProvideCoderException.class);
    pipeline.getCoderRegistry().getDefaultCoder(output.get(0).getTypeDescriptor());
  }
}
