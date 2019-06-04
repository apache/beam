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
package org.apache.beam.runners.direct;

import static org.junit.Assert.assertThat;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory.PTransformReplacement;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DirectGBKIntoKeyedWorkItemsOverrideFactory}. */
@RunWith(JUnit4.class)
public class DirectGroupByKeyOverrideFactoryTest {
  private DirectGroupByKeyOverrideFactory<String, Integer> factory =
      new DirectGroupByKeyOverrideFactory<>();

  @Test
  public void getInputSucceeds() {
    TestPipeline p = TestPipeline.create();
    PCollection<KV<String, Integer>> input =
        p.apply(
            Create.of(KV.of("foo", 1))
                .withCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of())));
    PCollection<KV<String, Iterable<Integer>>> grouped = input.apply(GroupByKey.create());
    AppliedPTransform<?, ?, ?> producer = DirectGraphs.getProducer(grouped);
    PTransformReplacement<
            PCollection<KV<String, Integer>>, PCollection<KV<String, Iterable<Integer>>>>
        replacement = factory.getReplacementTransform((AppliedPTransform) producer);
    assertThat(replacement.getInput(), Matchers.<PCollection<?>>equalTo(input));
  }
}
