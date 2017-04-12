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
import org.junit.Test;

import static org.junit.Assert.*;

public class UnionTest {
  @Test
  public void testBuild() {
    Flow flow = Flow.create("TEST");
    Dataset<String> left = Util.createMockDataset(flow, 2);
    Dataset<String> right = Util.createMockDataset(flow, 3);

    Dataset<String> unioned = Union.named("Union1")
            .of(left, right)
            .output();

    assertEquals(flow, unioned.getFlow());
    assertEquals(1, flow.size());

    Union union = (Union) flow.operators().iterator().next();
    assertEquals(flow, union.getFlow());
    assertEquals("Union1", union.getName());
    assertEquals(unioned, union.output());
  }

  @Test
  public void testBuild_ImplicitName() {
    Flow flow = Flow.create("TEST");
    Dataset<String> left = Util.createMockDataset(flow, 2);
    Dataset<String> right = Util.createMockDataset(flow, 3);

    Dataset<String> unioned = Union.of(left, right).output();

    Union union = (Union) flow.operators().iterator().next();
    assertEquals("Union", union.getName());
  }
}