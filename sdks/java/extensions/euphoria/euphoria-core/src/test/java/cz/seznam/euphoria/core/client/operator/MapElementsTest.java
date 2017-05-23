/**
 * Copyright 2016-2017 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.Context;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class MapElementsTest {

  @Test
  public void testBuild() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 1);

    Dataset<String> mapped = MapElements.named("Map1")
       .of(dataset)
       .using(s -> s)
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
  public void testBuild_WithCounters() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 1);

    Dataset<String> mapped = MapElements.named("Map1")
            .of(dataset)
            .using((String input, Context context) -> {
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

    Dataset<String> mapped = MapElements.of(dataset)
            .using(s -> s)
            .output();

    MapElements map = (MapElements) flow.operators().iterator().next();
    assertEquals("MapElements", map.getName());
  }
}