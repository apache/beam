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
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.hint.SizeHint;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypePropagationAssert;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Test;

/** Test operator MapElement. */
public class MapElementsTest {

  @Test
  public void testBuild() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 1);

    Dataset<String> mapped = MapElements.named("Map1").of(dataset).using(s -> s).output();

    assertEquals(flow, mapped.getFlow());
    assertEquals(1, flow.size());

    MapElements map = (MapElements) flow.operators().iterator().next();
    assertEquals(flow, map.getFlow());
    assertEquals("Map1", map.getName());
    assertNotNull(map.getMapper());
    assertEquals(mapped, map.output());
  }

  @Test
  public void testBuild_WithCounters() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 1);

    Dataset<String> mapped =
        MapElements.named("Map1")
            .of(dataset)
            .using(
                (input, context) -> {
                  // use simple counter
                  context.getCounter("my-counter").increment();

                  return input.toLowerCase();
                })
            .output();

    assertEquals(flow, mapped.getFlow());
    assertEquals(1, flow.size());

    MapElements map = (MapElements) flow.operators().iterator().next();
    assertEquals(flow, map.getFlow());
    assertEquals("Map1", map.getName());
    assertNotNull(map.getMapper());
    assertEquals(mapped, map.output());
  }

  @Test
  public void testBuild_ImplicitName() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 1);

    Dataset<String> mapped = MapElements.of(dataset).using(s -> s).output();

    MapElements map = (MapElements) flow.operators().iterator().next();
    assertEquals("MapElements", map.getName());
  }

  @Test
  public void testBuild_Hints() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 1);

    Dataset<String> dataSetWithHint =
        MapElements.of(dataset).using(i -> i).output(SizeHint.FITS_IN_MEMORY);

    assertTrue(dataSetWithHint.getProducer().getHints().contains(SizeHint.FITS_IN_MEMORY));
    assertEquals(1, dataSetWithHint.getProducer().getHints().size());

    Dataset<String> dataSetWithoutHint = MapElements.of(dataset).using(i -> i).output();
    assertEquals(0, dataSetWithoutHint.getProducer().getHints().size());
  }

  @Test
  public void testTypePropagation() {
    Flow flow1 = Flow.create("TEST1");
    Dataset<Integer> input = Util.createMockDataset(flow1, 2);

    TypeDescriptor<String> outputType = TypeDescriptors.strings();

    Dataset<String> mappedElements =
        MapElements.named("Int2Str").of(input).using(String::valueOf, outputType).output();

    MapElements map = (MapElements) flow1.operators().iterator().next();
    TypePropagationAssert.assertOperatorTypeAwareness(map, outputType);
  }
}
