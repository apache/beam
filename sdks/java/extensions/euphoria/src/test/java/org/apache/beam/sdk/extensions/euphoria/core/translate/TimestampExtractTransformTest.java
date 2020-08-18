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
package org.apache.beam.sdk.extensions.euphoria.core.translate;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.CountByKey;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test suite for {@link TimetampExtractTransform}. */
@RunWith(JUnit4.class)
public class TimestampExtractTransformTest {

  @SuppressWarnings("unchecked")
  @Test(timeout = 10000)
  public void testTransform() {
    Pipeline p = Pipeline.create();
    PCollection<Integer> input = p.apply(Create.of(1, 2, 3));
    PCollection<KV<Integer, Long>> result =
        input.apply(
            TimestampExtractTransform.of(
                in -> CountByKey.of(in).keyBy(KV::getValue, TypeDescriptors.integers()).output()));
    PAssert.that(result).containsInAnyOrder(KV.of(1, 1L), KV.of(2, 1L), KV.of(3, 1L));
    p.run().waitUntilFinish();
  }
}
