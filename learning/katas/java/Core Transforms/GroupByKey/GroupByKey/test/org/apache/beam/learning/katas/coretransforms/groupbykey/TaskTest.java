/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.beam.learning.katas.coretransforms.groupbykey;

import static org.apache.beam.learning.katas.util.ContainsKvs.containsKvs;

import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class TaskTest {

  @Rule
  public final transient TestPipeline testPipeline = TestPipeline.create();

  @Test
  public void groupByKey() {
    Create.Values<String> values = Create.of("apple", "ball", "car", "bear", "cheetah", "ant");
    PCollection<String> numbers = testPipeline.apply(values);

    PCollection<KV<String, Iterable<String>>> results = Task.applyTransform(numbers);

    PAssert.that(results)
        .satisfies(
            containsKvs(
                KV.of("a", ImmutableList.of("apple", "ant")),
                KV.of("b", ImmutableList.of("ball", "bear")),
                KV.of("c", ImmutableList.of("car", "cheetah"))
            )
        );

    testPipeline.run().waitUntilFinish();
  }

}