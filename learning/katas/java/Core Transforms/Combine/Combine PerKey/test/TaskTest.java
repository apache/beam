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

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class TaskTest {

  @Rule
  public TestPipeline testPipeline = TestPipeline.create();

  @SuppressWarnings("unchecked")
  @Test
  public void combine_combineFn() {
    Create.Values<KV<String, Integer>> values = Create.of(
        KV.of(Task.PLAYER_1, 15), KV.of(Task.PLAYER_2, 10), KV.of(Task.PLAYER_1, 100),
        KV.of(Task.PLAYER_3, 25), KV.of(Task.PLAYER_2, 75)
    );
    PCollection<KV<String, Integer>> numbers = testPipeline.apply(values);

    PCollection<KV<String, Integer>> results = Task.applyTransform(numbers);

    PAssert.that(results)
        .containsInAnyOrder(
            KV.of(Task.PLAYER_1, 115), KV.of(Task.PLAYER_2, 85), KV.of(Task.PLAYER_3, 25)
        );

    testPipeline.run().waitUntilFinish();
  }

}