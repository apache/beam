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
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Sets;
import java.util.Set;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.MockStreamDataSource;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.VoidSink;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.hint.OutputHint;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.hint.SizeHint;
import org.apache.beam.sdk.extensions.euphoria.core.executor.FlowUnfolder;
import org.apache.beam.sdk.extensions.euphoria.core.executor.graph.DAG;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.values.KV;
import org.junit.Test;

/** Test usage of hints in different operators. */
public class HintTest {

  /** Test every node in DAG which was unfolded from original operator, if preserves hints. */
  @Test
  @SuppressWarnings("unchecked")
  public void testHintsAfterUnfold() {
    Flow flow = Flow.create(getClass().getSimpleName());
    Dataset<Object> input = flow.createInput(new MockStreamDataSource<>());

    Dataset<Object> mapped =
        MapElements.named("mapElementsFitInMemoryHint")
            .of(input)
            .using(e -> e)
            .output(SizeHint.FITS_IN_MEMORY);
    Dataset<KV<Object, Long>> reduced =
        ReduceByKey.named("reduceByKeyTwoHints")
            .of(mapped)
            .keyBy(e -> e)
            .reduceBy(values -> 1L)
            .windowBy(FixedWindows.of(org.joda.time.Duration.standardSeconds(1)))
            .triggeredBy(DefaultTrigger.of())
            .discardingFiredPanes()
            .output(new Util.TestHint(), new Util.TestHint2());

    Dataset<Object> mapped2 =
        MapElements.named("mapElementsTestHint2")
            .of(reduced)
            .using(KV::getKey)
            .output(new Util.TestHint2());
    mapped2.persist(new VoidSink<>());

    Dataset<KV<Object, Long>> output =
        Join.named("joinHint")
            .of(mapped, reduced)
            .by(e -> e, KV::getKey)
            .using((Object l, KV<Object, Long> r, Collector<Long> c) -> c.collect(r.getValue()))
            .windowBy(FixedWindows.of(org.joda.time.Duration.standardSeconds(1)))
            .triggeredBy(DefaultTrigger.of())
            .discardingFiredPanes()
            .output(new Util.TestHint());

    output.persist(new VoidSink<>());

    DAG<Operator<?, ?>> unfolded = FlowUnfolder.unfold(flow, Operators.getBasicOps());

    testNodesByName(
        unfolded, "mapElementsFitInMemoryHint", 1, Sets.newHashSet(SizeHint.FITS_IN_MEMORY));

    testNodesByName(unfolded, "mapElementsTestHint2", 1, Sets.newHashSet(new Util.TestHint2()));

    testNodesByName(
        unfolded,
        "reduceByKeyTwoHints",
        2,
        Sets.newHashSet(new Util.TestHint(), new Util.TestHint2()));

    testNodesByName(
        unfolded, "joinHint::ReduceStateByKey", 1, Sets.newHashSet(new Util.TestHint()));
  }

  private void testNodesByName(
      DAG<Operator<?, ?>> unfolded,
      String name,
      int expectedHintCount,
      Set<OutputHint> expectedHints) {
    unfolded
        .nodes()
        .filter(node -> node.getName().equalsIgnoreCase(name))
        .forEach(
            operator -> {
              assertEquals(expectedHintCount, operator.getHints().size());
              assertTrue(expectedHints.containsAll(operator.getHints()));
            });
  }
}
