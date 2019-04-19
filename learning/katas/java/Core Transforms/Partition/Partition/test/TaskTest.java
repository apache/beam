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
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.junit.Rule;
import org.junit.Test;

public class TaskTest {

  @Rule
  public TestPipeline testPipeline = TestPipeline.create();

  @Test
  public void groupByKey() {
    PCollection<Integer> numbers =
        testPipeline.apply(
            Create.of(1, 2, 3, 4, 5, 100, 110, 150, 250)
        );

    PCollectionList<Integer> results = Task.applyTransform(numbers);

    PAssert.that(results.get(0))
        .containsInAnyOrder(110, 150, 250);

    PAssert.that(results.get(1))
        .containsInAnyOrder(1, 2, 3, 4, 5, 100);

    testPipeline.run().waitUntilFinish();
  }

}