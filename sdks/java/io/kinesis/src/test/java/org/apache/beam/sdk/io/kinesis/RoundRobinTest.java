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
package org.apache.beam.sdk.io.kinesis;

import static com.google.common.collect.Lists.newArrayList;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;

import org.junit.Test;

/**
 * Tests {@link RoundRobin}.
 */
public class RoundRobinTest {

  @Test(expected = IllegalArgumentException.class)
  public void doesNotAllowCreationWithEmptyCollection() {
    new RoundRobin<>(Collections.emptyList());
  }

  @Test
  public void goesThroughElementsInCycle() {
    List<String> input = newArrayList("a", "b", "c");

    RoundRobin<String> roundRobin = new RoundRobin<>(newArrayList(input));

    input.addAll(input);  // duplicate the input
    for (String element : input) {
      assertThat(roundRobin.getCurrent()).isEqualTo(element);
      assertThat(roundRobin.getCurrent()).isEqualTo(element);
      roundRobin.moveForward();
    }
  }

  @Test
  public void usualIteratorGoesThroughElementsOnce() {
    List<String> input = newArrayList("a", "b", "c");

    RoundRobin<String> roundRobin = new RoundRobin<>(input);
    assertThat(roundRobin).hasSize(3).containsOnly(input.toArray(new String[0]));
  }
}
