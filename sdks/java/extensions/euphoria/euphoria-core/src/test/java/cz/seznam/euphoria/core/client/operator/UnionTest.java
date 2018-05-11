/*
 * Copyright 2016-2018 Seznam.cz, a.s.
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

import static org.junit.Assert.assertEquals;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import org.junit.Test;

/** Test behavior of operator {@code Union}. */
public class UnionTest {

  @Test
  public void testBuild() {
    final Flow flow = Flow.create("TEST");
    final Dataset<String> left = Util.createMockDataset(flow, 2);
    final Dataset<String> right = Util.createMockDataset(flow, 3);

    final Dataset<String> unioned = Union.named("Union1").of(left, right).output();

    assertEquals(flow, unioned.getFlow());
    assertEquals(1, flow.size());

    final Union union = (Union) flow.operators().iterator().next();
    assertEquals(flow, union.getFlow());
    assertEquals("Union1", union.getName());
    assertEquals(unioned, union.output());
    assertEquals(2, union.listInputs().size());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuild_OneDataSet() {
    final Flow flow = Flow.create("TEST");
    final Dataset<String> first = Util.createMockDataset(flow, 1);
    Union.named("Union1").of(first).output();
  }

  @Test
  public void testBuild_ThreeDataSet() {
    final Flow flow = Flow.create("TEST");
    final Dataset<String> first = Util.createMockDataset(flow, 1);
    final Dataset<String> second = Util.createMockDataset(flow, 2);
    final Dataset<String> third = Util.createMockDataset(flow, 3);

    final Dataset<String> unioned = Union.named("Union1").of(first, second, third).output();

    assertEquals(flow, unioned.getFlow());
    assertEquals(1, flow.size());

    final Union union = (Union) flow.operators().iterator().next();
    assertEquals(flow, union.getFlow());
    assertEquals("Union1", union.getName());
    assertEquals(unioned, union.output());
    assertEquals(3, union.listInputs().size());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuild_ThreeDataSet_oneFromDifferentFlow() {
    final Flow flow = Flow.create("TEST");
    final Flow flow2 = Flow.create("TEST2");
    final Dataset<String> first = Util.createMockDataset(flow, 1);
    final Dataset<String> second = Util.createMockDataset(flow, 2);
    final Dataset<String> third = Util.createMockDataset(flow2, 3);

    Union.named("Union1").of(first, second, third).output();
  }

  @Test
  public void testBuild_ImplicitName() {
    final Flow flow = Flow.create("TEST");
    final Dataset<String> left = Util.createMockDataset(flow, 2);
    final Dataset<String> right = Util.createMockDataset(flow, 3);

    Union.of(left, right).output();

    final Union union = (Union) flow.operators().iterator().next();
    assertEquals("Union", union.getName());
  }
}
