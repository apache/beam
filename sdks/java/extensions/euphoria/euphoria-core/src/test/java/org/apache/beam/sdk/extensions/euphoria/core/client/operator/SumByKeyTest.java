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
package org.apache.beam.sdk.extensions.euphoria.core.client.operator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Time;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;
import org.junit.Test;

/** Test behavior of operator {@code SumByKey}. */
public class SumByKeyTest {
  @Test
  public void testBuild() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    Dataset<Pair<String, Long>> counted =
        SumByKey.named("SumByKey1").of(dataset).keyBy(s -> s).output();

    assertEquals(flow, counted.getFlow());
    assertEquals(1, flow.size());

    SumByKey sum = (SumByKey) flow.operators().iterator().next();
    assertEquals(flow, sum.getFlow());
    assertEquals("SumByKey1", sum.getName());
    assertNotNull(sum.keyExtractor);
    assertEquals(counted, sum.output());
    assertNull(sum.getWindowing());
  }

  @Test
  public void testBuild_ImplicitName() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    SumByKey.of(dataset).keyBy(s -> s).output();

    SumByKey sum = (SumByKey) flow.operators().iterator().next();
    assertEquals("SumByKey", sum.getName());
  }

  @Test
  public void testBuild_Windowing() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    SumByKey.of(dataset)
        .keyBy(s -> s)
        .valueBy(s -> 1L)
        .windowBy(Time.of(Duration.ofHours(1)))
        .output();

    SumByKey sum = (SumByKey) flow.operators().iterator().next();
    assertTrue(sum.getWindowing() instanceof Time);
  }

  @Test
  public void testWindow_applyIf() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    SumByKey.of(dataset)
        .keyBy(s -> s)
        .valueBy(s -> 1L)
        .applyIf(true, b -> b.windowBy(Time.of(Duration.ofHours(1))))
        .output();

    SumByKey sum = (SumByKey) flow.operators().iterator().next();
    assertTrue(sum.getWindowing() instanceof Time);
  }
}
