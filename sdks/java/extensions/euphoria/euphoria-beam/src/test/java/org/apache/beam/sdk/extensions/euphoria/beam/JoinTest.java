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
package org.apache.beam.sdk.extensions.euphoria.beam;

import static java.util.Arrays.asList;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;

import java.util.Optional;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.ListDataSink;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.ListDataSource;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.FullJoin;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Join;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.LeftJoin;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.MapElements;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.RightJoin;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.hint.SizeHint;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;
import org.apache.beam.sdk.extensions.euphoria.testing.DatasetAssert;
import org.junit.Test;

/**
 * Simple test suite for Join operator.
 */
public class JoinTest {

  static <T> Dataset<T> addFitsInMemoryHint(Dataset<T> smallDataset) {
    return MapElements.named("smallSide")
        .of(smallDataset)
        .using(e -> e)
        .output(SizeHint.FITS_IN_MEMORY);
  }

  static void checkBrodcastHashJoinTranslatorUsage(Flow flow) {
    OperatorTranslator operatorTranslator =
        flow.operators()
            .stream()
            .filter(node -> node instanceof Join)
            .map(FlowTranslator::getTranslatorIfAvailable)
            .findFirst()
            .orElse(null);

    assertThat(operatorTranslator, instanceOf(BrodcastHashJoinTranslator.class));
  }

  static void checkRightJoin(
      Flow flow, Dataset<Pair<Integer, String>> left, Dataset<Pair<Integer, Integer>> right) {

    ListDataSink<Pair<Integer, Pair<String, Integer>>> output = ListDataSink.get();

    RightJoin.of(left, right)
        .by(Pair::getFirst, Pair::getFirst)
        .using(
            (Optional<Pair<Integer, String>> l,
                Pair<Integer, Integer> r,
                Collector<Pair<String, Integer>> c) ->
                c.collect(Pair.of(l.orElse(Pair.of(null, null)).getSecond(), r.getSecond())))
        .output()
        .persist(output);

    BeamRunnerWrapper executor = BeamRunnerWrapper.ofDirect();
    executor.executeSync(flow);

    DatasetAssert.unorderedEquals(
        output.getOutputs(),
        Pair.of(1, Pair.of("L v1", 1)),
        Pair.of(1, Pair.of("L v1", 10)),
        Pair.of(1, Pair.of("L v2", 1)),
        Pair.of(1, Pair.of("L v2", 10)),
        Pair.of(2, Pair.of("L v1", 20)),
        Pair.of(2, Pair.of("L v2", 20)),
        Pair.of(4, Pair.of(null, 40)));
  }

  static void checkLeftJoin(
      Flow flow, Dataset<Pair<Integer, String>> left, Dataset<Pair<Integer, Integer>> right) {

    ListDataSink<Pair<Integer, Pair<String, Integer>>> output = ListDataSink.get();

    LeftJoin.of(left, right)
        .by(Pair::getFirst, Pair::getFirst)
        .using(
            (Pair<Integer, String> l,
                Optional<Pair<Integer, Integer>> r,
                Collector<Pair<String, Integer>> c) ->
                c.collect(Pair.of(l.getSecond(), r.orElse(Pair.of(null, null)).getSecond())))
        .output()
        .persist(output);

    BeamRunnerWrapper executor = BeamRunnerWrapper.ofDirect();
    executor.executeSync(flow);

    DatasetAssert.unorderedEquals(
        output.getOutputs(),
        Pair.of(1, Pair.of("L v1", 1)),
        Pair.of(1, Pair.of("L v1", 10)),
        Pair.of(1, Pair.of("L v2", 1)),
        Pair.of(1, Pair.of("L v2", 10)),
        Pair.of(2, Pair.of("L v1", 20)),
        Pair.of(2, Pair.of("L v2", 20)),
        Pair.of(3, Pair.of("L v1", null)));
  }

  @Test
  public void simpleInnerJoinTest() {
    final Flow flow = Flow.create();

    ListDataSource<Pair<Integer, String>> left = getLeftDataSource();
    ListDataSource<Pair<Integer, Integer>> right = getRightDataSource();

    ListDataSink<Pair<Integer, Pair<String, Integer>>> output = ListDataSink.get();

    Join.of(flow.createInput(left), flow.createInput(right))
        .by(Pair::getFirst, Pair::getFirst)
        .using(
            (Pair<Integer, String> l,
                Pair<Integer, Integer> r,
                Collector<Pair<String, Integer>> c) ->
                c.collect(Pair.of(l.getSecond(), r.getSecond())))
        .output()
        .persist(output);

    BeamRunnerWrapper executor = BeamRunnerWrapper.ofDirect();
    executor.executeSync(flow);

    DatasetAssert.unorderedEquals(
        output.getOutputs(),
        Pair.of(1, Pair.of("L v1", 1)),
        Pair.of(1, Pair.of("L v1", 10)),
        Pair.of(1, Pair.of("L v2", 1)),
        Pair.of(1, Pair.of("L v2", 10)),
        Pair.of(2, Pair.of("L v1", 20)),
        Pair.of(2, Pair.of("L v2", 20)));
  }

  @Test
  public void simpleLeftJoinTest() {
    final Flow flow = Flow.create();

    ListDataSource<Pair<Integer, String>> left = getLeftDataSource();
    ListDataSource<Pair<Integer, Integer>> right = getRightDataSource();

    checkLeftJoin(flow, flow.createInput(left), flow.createInput(right));
  }

  @Test
  public void simpleRightJoinTest() {
    final Flow flow = Flow.create();

    ListDataSource<Pair<Integer, String>> left = getLeftDataSource();
    ListDataSource<Pair<Integer, Integer>> right = getRightDataSource();

    checkRightJoin(flow, flow.createInput(left), flow.createInput(right));
  }

  @Test
  public void simpleFullJoinTest() {
    final Flow flow = Flow.create();

    ListDataSource<Pair<Integer, String>> left = getLeftDataSource();
    ListDataSource<Pair<Integer, Integer>> right = getRightDataSource();

    ListDataSink<Pair<Integer, Pair<String, Integer>>> output = ListDataSink.get();

    FullJoin.of(flow.createInput(left), flow.createInput(right))
        .by(Pair::getFirst, Pair::getFirst)
        .using(
            (Optional<Pair<Integer, String>> l,
                Optional<Pair<Integer, Integer>> r,
                Collector<Pair<String, Integer>> c) ->
                c.collect(
                    Pair.of(
                        l.orElse(Pair.of(null, null)).getSecond(),
                        r.orElse(Pair.of(null, null)).getSecond())))
        .output()
        .persist(output);

    BeamRunnerWrapper executor = BeamRunnerWrapper.ofDirect();
    executor.executeSync(flow);

    DatasetAssert.unorderedEquals(
        output.getOutputs(),
        Pair.of(1, Pair.of("L v1", 1)),
        Pair.of(1, Pair.of("L v1", 10)),
        Pair.of(1, Pair.of("L v2", 1)),
        Pair.of(1, Pair.of("L v2", 10)),
        Pair.of(2, Pair.of("L v1", 20)),
        Pair.of(2, Pair.of("L v2", 20)),
        Pair.of(3, Pair.of("L v1", null)),
        Pair.of(4, Pair.of(null, 40)));
  }

  @Test
  public void simpleBroadcastHashRightJoinTest() {
    final Flow flow = Flow.create();

    ListDataSource<Pair<Integer, String>> left = getLeftDataSource();
    ListDataSource<Pair<Integer, Integer>> right = getRightDataSource();

    final Dataset<Pair<Integer, String>> smallLeftSide =
        addFitsInMemoryHint(flow.createInput(left));

    checkRightJoin(flow, smallLeftSide, flow.createInput(right));

    checkBrodcastHashJoinTranslatorUsage(flow);
  }

  @Test
  public void simpleBroadcastHashLefttJoinTest() {
    final Flow flow = Flow.create();

    ListDataSource<Pair<Integer, String>> left = getLeftDataSource();
    ListDataSource<Pair<Integer, Integer>> right = getRightDataSource();

    final Dataset<Pair<Integer, Integer>> smallRightSide =
        addFitsInMemoryHint(flow.createInput(right));

    checkLeftJoin(flow, flow.createInput(left), smallRightSide);

    checkBrodcastHashJoinTranslatorUsage(flow);
  }

  ListDataSource<Pair<Integer, String>> getLeftDataSource() {
    return ListDataSource.bounded(
        asList(
            Pair.of(1, "L v1"),
            Pair.of(1, "L v2"),
            Pair.of(2, "L v1"),
            Pair.of(2, "L v2"),
            Pair.of(3, "L v1")));
  }

  ListDataSource<Pair<Integer, Integer>> getRightDataSource() {
    return ListDataSource.bounded(
        asList(Pair.of(1, 1), Pair.of(1, 10), Pair.of(2, 20), Pair.of(4, 40)));
  }
}
