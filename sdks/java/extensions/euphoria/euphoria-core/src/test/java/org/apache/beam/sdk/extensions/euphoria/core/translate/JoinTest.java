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
package org.apache.beam.sdk.extensions.euphoria.core.translate;

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
import org.apache.beam.sdk.extensions.euphoria.testing.DatasetAssert;
import org.apache.beam.sdk.values.KV;
import org.junit.Test;

/** Simple test suite for Join operator. */
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

    assertThat(operatorTranslator, instanceOf(BroadcastHashJoinTranslator.class));
  }

  static void checkRightJoin(
      Flow flow, Dataset<KV<Integer, String>> left, Dataset<KV<Integer, Integer>> right) {

    ListDataSink<KV<Integer, KV<String, Integer>>> output = ListDataSink.get();

    RightJoin.of(left, right)
        .by(KV::getKey, KV::getKey)
        .using(
            (Optional<KV<Integer, String>> l,
                KV<Integer, Integer> r,
                Collector<KV<String, Integer>> c) ->
                c.collect(KV.of(l.orElse(KV.of(null, null)).getValue(), r.getValue())))
        .output()
        .persist(output);

    BeamRunnerWrapper executor = BeamRunnerWrapper.ofDirect();
    executor.executeSync(flow);

    DatasetAssert.unorderedEquals(
        output.getOutputs(),
        KV.of(1, KV.of("L v1", 1)),
        KV.of(1, KV.of("L v1", 10)),
        KV.of(1, KV.of("L v2", 1)),
        KV.of(1, KV.of("L v2", 10)),
        KV.of(2, KV.of("L v1", 20)),
        KV.of(2, KV.of("L v2", 20)),
        KV.of(4, KV.of(null, 40)));
  }

  static void checkRightJoinOutputsTwice(
      Flow flow, Dataset<KV<Integer, String>> left, Dataset<KV<Integer, Integer>> right) {

    ListDataSink<KV<Integer, KV<String, Integer>>> output = ListDataSink.get();

    RightJoin.of(left, right)
        .by(KV::getKey, KV::getKey)
        .using(
            (Optional<KV<Integer, String>> l,
                KV<Integer, Integer> r,
                Collector<KV<String, Integer>> c) -> {
              KV<String, Integer> ouputElement =
                  KV.of(l.orElse(KV.of(null, null)).getValue(), r.getValue());
              c.collect(ouputElement);
              c.collect(ouputElement);
            })
        .output()
        .persist(output);

    BeamRunnerWrapper executor = BeamRunnerWrapper.ofDirect();
    executor.executeSync(flow);

    DatasetAssert.unorderedEquals(
        output.getOutputs(),
        KV.of(1, KV.of("L v1", 1)),
        KV.of(1, KV.of("L v1", 1)),
        KV.of(1, KV.of("L v1", 10)),
        KV.of(1, KV.of("L v1", 10)),
        KV.of(1, KV.of("L v2", 1)),
        KV.of(1, KV.of("L v2", 1)),
        KV.of(1, KV.of("L v2", 10)),
        KV.of(1, KV.of("L v2", 10)),
        KV.of(2, KV.of("L v1", 20)),
        KV.of(2, KV.of("L v1", 20)),
        KV.of(2, KV.of("L v2", 20)),
        KV.of(2, KV.of("L v2", 20)),
        KV.of(4, KV.of(null, 40)),
        KV.of(4, KV.of(null, 40)));
  }

  static void checkLeftJoin(
      Flow flow, Dataset<KV<Integer, String>> left, Dataset<KV<Integer, Integer>> right) {

    ListDataSink<KV<Integer, KV<String, Integer>>> output = ListDataSink.get();

    LeftJoin.of(left, right)
        .by(KV::getKey, KV::getKey)
        .using(
            (KV<Integer, String> l,
                Optional<KV<Integer, Integer>> r,
                Collector<KV<String, Integer>> c) ->
                c.collect(KV.of(l.getValue(), r.orElse(KV.of(null, null)).getValue())))
        .output()
        .persist(output);

    BeamRunnerWrapper executor = BeamRunnerWrapper.ofDirect();
    executor.executeSync(flow);

    DatasetAssert.unorderedEquals(
        output.getOutputs(),
        KV.of(1, KV.of("L v1", 1)),
        KV.of(1, KV.of("L v1", 10)),
        KV.of(1, KV.of("L v2", 1)),
        KV.of(1, KV.of("L v2", 10)),
        KV.of(2, KV.of("L v1", 20)),
        KV.of(2, KV.of("L v2", 20)),
        KV.of(3, KV.of("L v1", null)));
  }

  static void checkLeftJoinOutputTwice(
      Flow flow, Dataset<KV<Integer, String>> left, Dataset<KV<Integer, Integer>> right) {

    ListDataSink<KV<Integer, KV<String, Integer>>> output = ListDataSink.get();

    LeftJoin.of(left, right)
        .by(KV::getKey, KV::getKey)
        .using(
            (KV<Integer, String> l,
                Optional<KV<Integer, Integer>> r,
                Collector<KV<String, Integer>> c) -> {
              KV<String, Integer> outElement =
                  KV.of(l.getValue(), r.orElse(KV.of(null, null)).getValue());
              c.collect(outElement);
              c.collect(outElement);
            })
        .output()
        .persist(output);

    BeamRunnerWrapper executor = BeamRunnerWrapper.ofDirect();
    executor.executeSync(flow);

    DatasetAssert.unorderedEquals(
        output.getOutputs(),
        KV.of(1, KV.of("L v1", 1)),
        KV.of(1, KV.of("L v1", 1)),
        KV.of(1, KV.of("L v1", 10)),
        KV.of(1, KV.of("L v1", 10)),
        KV.of(1, KV.of("L v2", 1)),
        KV.of(1, KV.of("L v2", 1)),
        KV.of(1, KV.of("L v2", 10)),
        KV.of(1, KV.of("L v2", 10)),
        KV.of(2, KV.of("L v1", 20)),
        KV.of(2, KV.of("L v1", 20)),
        KV.of(2, KV.of("L v2", 20)),
        KV.of(2, KV.of("L v2", 20)),
        KV.of(3, KV.of("L v1", null)),
        KV.of(3, KV.of("L v1", null)));
  }

  @Test
  public void simpleInnerJoinTest() {
    final Flow flow = Flow.create();

    ListDataSource<KV<Integer, String>> left = getLeftDataSource();
    ListDataSource<KV<Integer, Integer>> right = getRightDataSource();

    ListDataSink<KV<Integer, KV<String, Integer>>> output = ListDataSink.get();

    Join.of(flow.createInput(left), flow.createInput(right))
        .by(KV::getKey, KV::getKey)
        .using(
            (KV<Integer, String> l, KV<Integer, Integer> r, Collector<KV<String, Integer>> c) ->
                c.collect(KV.of(l.getValue(), r.getValue())))
        .output()
        .persist(output);

    BeamRunnerWrapper executor = BeamRunnerWrapper.ofDirect();
    executor.executeSync(flow);

    DatasetAssert.unorderedEquals(
        output.getOutputs(),
        KV.of(1, KV.of("L v1", 1)),
        KV.of(1, KV.of("L v1", 10)),
        KV.of(1, KV.of("L v2", 1)),
        KV.of(1, KV.of("L v2", 10)),
        KV.of(2, KV.of("L v1", 20)),
        KV.of(2, KV.of("L v2", 20)));
  }

  @Test
  public void simpleLeftJoinTest() {
    final Flow flow = Flow.create();

    ListDataSource<KV<Integer, String>> left = getLeftDataSource();
    ListDataSource<KV<Integer, Integer>> right = getRightDataSource();

    checkLeftJoin(flow, flow.createInput(left), flow.createInput(right));
  }

  @Test
  public void simpleRightJoinTest() {
    final Flow flow = Flow.create();

    ListDataSource<KV<Integer, String>> left = getLeftDataSource();
    ListDataSource<KV<Integer, Integer>> right = getRightDataSource();

    checkRightJoin(flow, flow.createInput(left), flow.createInput(right));
  }

  @Test
  public void simpleFullJoinTest() {
    final Flow flow = Flow.create();

    ListDataSource<KV<Integer, String>> left = getLeftDataSource();
    ListDataSource<KV<Integer, Integer>> right = getRightDataSource();

    ListDataSink<KV<Integer, KV<String, Integer>>> output = ListDataSink.get();

    FullJoin.of(flow.createInput(left), flow.createInput(right))
        .by(KV::getKey, KV::getKey)
        .using(
            (Optional<KV<Integer, String>> l,
                Optional<KV<Integer, Integer>> r,
                Collector<KV<String, Integer>> c) ->
                c.collect(
                    KV.of(
                        l.orElse(KV.of(null, null)).getValue(),
                        r.orElse(KV.of(null, null)).getValue())))
        .output()
        .persist(output);

    BeamRunnerWrapper executor = BeamRunnerWrapper.ofDirect();
    executor.executeSync(flow);

    DatasetAssert.unorderedEquals(
        output.getOutputs(),
        KV.of(1, KV.of("L v1", 1)),
        KV.of(1, KV.of("L v1", 10)),
        KV.of(1, KV.of("L v2", 1)),
        KV.of(1, KV.of("L v2", 10)),
        KV.of(2, KV.of("L v1", 20)),
        KV.of(2, KV.of("L v2", 20)),
        KV.of(3, KV.of("L v1", null)),
        KV.of(4, KV.of(null, 40)));
  }

  @Test
  public void simpleBroadcastHashRightJoinTest() {
    final Flow flow = Flow.create();

    ListDataSource<KV<Integer, String>> left = getLeftDataSource();
    ListDataSource<KV<Integer, Integer>> right = getRightDataSource();

    final Dataset<KV<Integer, String>> smallLeftSide = addFitsInMemoryHint(flow.createInput(left));

    checkRightJoin(flow, smallLeftSide, flow.createInput(right));

    checkBrodcastHashJoinTranslatorUsage(flow);
  }

  @Test
  public void simpleBroadcastHashLeftJoinTest() {
    final Flow flow = Flow.create();

    ListDataSource<KV<Integer, String>> left = getLeftDataSource();
    ListDataSource<KV<Integer, Integer>> right = getRightDataSource();

    final Dataset<KV<Integer, Integer>> smallRightSide =
        addFitsInMemoryHint(flow.createInput(right));

    checkLeftJoin(flow, flow.createInput(left), smallRightSide);

    checkBrodcastHashJoinTranslatorUsage(flow);
  }

  @Test
  public void broadcastHashRightJoinMultipleOutputsTest() {
    final Flow flow = Flow.create();

    ListDataSource<KV<Integer, String>> left = getLeftDataSource();
    ListDataSource<KV<Integer, Integer>> right = getRightDataSource();

    final Dataset<KV<Integer, String>> smallLeftSide = addFitsInMemoryHint(flow.createInput(left));

    checkRightJoinOutputsTwice(flow, smallLeftSide, flow.createInput(right));

    checkBrodcastHashJoinTranslatorUsage(flow);
  }

  @Test
  public void simpleBroadcastHashLeftJoinMultipleOutputsTest() {
    final Flow flow = Flow.create();

    ListDataSource<KV<Integer, String>> left = getLeftDataSource();
    ListDataSource<KV<Integer, Integer>> right = getRightDataSource();

    final Dataset<KV<Integer, Integer>> smallRightSide =
        addFitsInMemoryHint(flow.createInput(right));

    checkLeftJoinOutputTwice(flow, flow.createInput(left), smallRightSide);

    checkBrodcastHashJoinTranslatorUsage(flow);
  }

  ListDataSource<KV<Integer, String>> getLeftDataSource() {
    return ListDataSource.bounded(
        asList(
            KV.of(1, "L v1"),
            KV.of(1, "L v2"),
            KV.of(2, "L v1"),
            KV.of(2, "L v2"),
            KV.of(3, "L v1")));
  }

  ListDataSource<KV<Integer, Integer>> getRightDataSource() {
    return ListDataSource.bounded(asList(KV.of(1, 1), KV.of(1, 10), KV.of(2, 20), KV.of(4, 40)));
  }
}
