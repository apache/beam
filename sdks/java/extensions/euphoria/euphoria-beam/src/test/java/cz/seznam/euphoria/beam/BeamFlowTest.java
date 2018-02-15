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
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.testing.DatasetAssert;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Test;

/**
 * Test for {@link BeamFlow}.
 */
public class BeamFlowTest {

  private PipelineOptions options() {
    String[] args = {"--runner=DirectRunner"};
    return PipelineOptionsFactory.fromArgs(args).as(PipelineOptions.class);
  }

  @Test
  public void testPipelineExec() {
    BeamFlow flow = BeamFlow.create();
    ListDataSource<Integer> source = ListDataSource.bounded(Arrays.asList(
        1, 2, 3, 4, 5));
    ListDataSink<Integer> sink = ListDataSink.get();
    Dataset<Integer> input = flow.createInput(source);
    MapElements.of(input)
        .using(e -> e + 1)
        .output()
        .persist(sink);
    Pipeline pipeline = flow.asPipeline(options());
    pipeline.run().waitUntilFinish();
    DatasetAssert.unorderedEquals(sink.getOutputs(), 2, 3, 4, 5, 6);
  }

  @Test
  public void testPipelineFromBeam() {
    List<Integer> inputs = Arrays.asList(1, 2, 3, 4, 5);
    Pipeline pipeline = Pipeline.create(options());
    BeamFlow flow = BeamFlow.create(pipeline);
    PCollection<Integer> input = pipeline.apply(Create.of(inputs))
        .setTypeDescriptor(TypeDescriptor.of(Integer.class));
    Dataset<Integer> ds = flow.wrapped(input);
    ListDataSink<Integer> sink = ListDataSink.get();
    MapElements.of(ds)
        .using(e -> e + 1)
        .output()
        .persist(sink);
    pipeline.run().waitUntilFinish();
    DatasetAssert.unorderedEquals(sink.getOutputs(), 2, 3, 4, 5, 6);
  }

  @Test
  public void testPipelineToAndFromBeam() {
    List<Integer> inputs = Arrays.asList(1, 2, 3, 4, 5);
    Pipeline pipeline = Pipeline.create(options());
    BeamFlow flow = BeamFlow.create(pipeline);
    PCollection<Integer> input = pipeline.apply(Create.of(inputs))
        .setTypeDescriptor(TypeDescriptor.of(Integer.class));
    Dataset<Integer> ds = flow.wrapped(input);
    Dataset<Integer> output = FlatMap.of(ds)
        .using((Integer e, Collector<Integer> c) -> c.collect(e + 1))
        .output();
    PCollection<Integer> unwrapped = flow.unwrapped(output);
    PAssert.that(unwrapped)
        .containsInAnyOrder(2, 3, 4, 5, 6);
    pipeline.run();
  }

}
