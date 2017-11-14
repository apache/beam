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
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.flow.Flow;
import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.*;

public class DistinctTest {

  @Test
  public void testBuild() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    Time<String> windowing = Time.of(Duration.ofHours(1));
    Dataset<String> uniq =
        Distinct.named("Distinct1")
            .of(dataset)
            .windowBy(windowing)
            .output();

    assertEquals(flow, uniq.getFlow());
    assertEquals(1, flow.size());

    Distinct distinct = (Distinct) flow.operators().iterator().next();
    assertEquals(flow, distinct.getFlow());
    assertEquals("Distinct1", distinct.getName());
    assertEquals(uniq, distinct.output());
    assertSame(windowing, distinct.getWindowing());
  }

  @Test
  public void testBuild_ImplicitName() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    Dataset<String> uniq = Distinct.of(dataset).output();

    Distinct distinct = (Distinct) flow.operators().iterator().next();
    assertEquals("Distinct", distinct.getName());
  }

  @Test
  public void testBuild_Windowing() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    Dataset<String> uniq = Distinct.of(dataset)
            .windowBy(Time.of(Duration.ofHours(1)))
            .output();

    Distinct distinct = (Distinct) flow.operators().iterator().next();
    assertTrue(distinct.getWindowing() instanceof Time);
  }

}