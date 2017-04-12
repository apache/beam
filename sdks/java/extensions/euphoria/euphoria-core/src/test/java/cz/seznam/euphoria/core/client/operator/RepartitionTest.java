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
import cz.seznam.euphoria.core.client.dataset.partitioning.HashPartitioner;
import cz.seznam.euphoria.core.client.flow.Flow;
import org.junit.Test;

import static org.junit.Assert.*;

public class RepartitionTest {
  @Test
  public void testBuild() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 3);

    Dataset<String> partitioned = Repartition.named("Repartition1")
            .of(dataset)
            .setNumPartitions(5)
            .setPartitioner(new HashPartitioner<>())
            .output();

    assertEquals(flow, partitioned.getFlow());
    assertEquals(1, flow.size());

    Repartition repartition = (Repartition) flow.operators().iterator().next();
    assertEquals(flow, repartition.getFlow());
    assertEquals("Repartition1", repartition.getName());
    assertEquals(partitioned, repartition.output());

    // default partitioning used
    assertTrue(!repartition.getPartitioning().hasDefaultPartitioner());
    assertTrue(repartition.getPartitioning().getPartitioner() instanceof HashPartitioner);
    assertEquals(5, repartition.getPartitioning().getNumPartitions());
  }

  @Test
  public void testBuild_ImplicitName() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 1);

    Dataset<String> partitioned = Repartition.of(dataset)
            .setNumPartitions(5)
            .output();

    Repartition repartition = (Repartition) flow.operators().iterator().next();
    assertEquals("Repartition", repartition.getName());
  }
}