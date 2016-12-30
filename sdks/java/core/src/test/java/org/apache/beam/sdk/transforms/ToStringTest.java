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

package org.apache.beam.sdk.transforms;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.RunnableOnService;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link ToString} transform.
 */
@RunWith(JUnit4.class)
public class ToStringTest {
  @Rule
  public final TestPipeline p = TestPipeline.create();

  @Test
  @Category(RunnableOnService.class)
  public void testToStringElement() {
    Integer[] ints = {1, 2, 3, 4, 5};
    PCollection<Integer> input = p.apply(Create.of(Arrays.asList(ints)));
    PCollection<String> output = input.apply(ToString.<Integer>element());
    PAssert.that(output).containsInAnyOrder(toStringList(ints));
    p.run();
  }

  private List<String> toStringList(Object[] ints) {
    List<String> ll = new ArrayList<>(ints.length);
    for (Object i : ints) {
      ll.add(i.toString());
    }
    return ll;
  }
}
