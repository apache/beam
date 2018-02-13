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
package cz.seznam.euphoria.beam;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.AssignEventTime;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.core.testing.DatasetAssert;
import java.time.Duration;
import java.util.Arrays;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;

/**
 * Simple test suite for RBK.
 */
public class ReduceByKeyTest {

  private BeamExecutor createExecutor() {
    String[] args = {"--runner=DirectRunner"};
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(PipelineOptions.class);
    return new BeamExecutor(options);
  }

  @Test
  public void tsetSimpleRBK() {
    final Flow flow = Flow.create();

    final ListDataSource<Integer> input = ListDataSource.unbounded(
        Arrays.asList(1, 2, 3),
        Arrays.asList(2, 3, 4));

    final ListDataSink<Pair<Integer, Integer>> output = ListDataSink.get();

    ReduceByKey.of(flow.createInput(input, e -> 1000L * e))
        .keyBy(i -> i % 2)
        .reduceBy(Sums.ofInts())
        .windowBy(Time.of(Duration.ofHours(1)))
        .output()
        .persist(output);

    BeamExecutor executor = createExecutor();
    executor.execute(flow);

    DatasetAssert.unorderedEquals(
        output.getOutputs(),
        Pair.of(0, 8), Pair.of(1, 7));
  }

  @Test
  public void testEventTime() {

    Flow flow = Flow.create();
    ListDataSource<Pair<Integer, Long>> source = ListDataSource.unbounded(Arrays.asList(
        Pair.of(1, 300L), Pair.of(2, 600L), Pair.of(3, 900L),
        Pair.of(2, 1300L), Pair.of(3, 1600L), Pair.of(1, 1900L),
        Pair.of(3, 2300L), Pair.of(2, 2600L), Pair.of(1, 2900L),
        Pair.of(2, 3300L),
        Pair.of(2, 300L), Pair.of(4, 600L), Pair.of(3, 900L),
        Pair.of(4, 1300L), Pair.of(2, 1600L), Pair.of(3, 1900L),
        Pair.of(4, 2300L), Pair.of(1, 2600L), Pair.of(3, 2900L),
        Pair.of(4, 3300L), Pair.of(3, 3600L)));

    ListDataSink<Pair<Integer, Long>> sink = ListDataSink.get();
    Dataset<Pair<Integer, Long>> input = flow.createInput(source);
    input = AssignEventTime.of(input).using(Pair::getSecond).output();
    ReduceByKey.of(input)
        .keyBy(Pair::getFirst)
        .valueBy(e -> 1L)
        .combineBy(Sums.ofLongs())
        .windowBy(Time.of(Duration.ofSeconds(1)))
        .output()
        .persist(sink);

    BeamExecutor executor = createExecutor();
    executor.execute(flow);

    DatasetAssert.unorderedEquals(sink.getOutputs(),
            Pair.of(2, 2L), Pair.of(4, 1L),  // first window
            Pair.of(2, 2L), Pair.of(4, 1L),  // second window
            Pair.of(2, 1L), Pair.of(4, 1L),  // third window
            Pair.of(2, 1L), Pair.of(4, 1L),  // fourth window
            Pair.of(1, 1L), Pair.of(3, 2L),  // first window
            Pair.of(1, 1L), Pair.of(3, 2L),  // second window
            Pair.of(1, 2L), Pair.of(3, 2L),  // third window
            Pair.of(3, 1L));                 // fourth window

  }

}
