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

import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.ListDataSink;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.ListDataSource;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.MapElements;
import org.apache.beam.sdk.extensions.euphoria.testing.DatasetAssert;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

/**
 * Test {@code FlatMap} operator's integration with beam.
 */
public class FlatMapTest {

  @Test
  public void testSimpleMap() throws ExecutionException, InterruptedException {

    final Flow flow = Flow.create();
    String[] args = {"--runner=DirectRunner"};
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(PipelineOptions.class);

    final ListDataSource<Integer> input =
        ListDataSource.unbounded(Arrays.asList(1, 2, 3), Arrays.asList(2, 3, 4));

    final ListDataSink<Integer> output = ListDataSink.get();

    MapElements.of(flow.createInput(input)).using(i -> i + 1).output().persist(output);

    BeamExecutor executor = new BeamExecutor(options);
    executor.execute(flow);

    DatasetAssert.unorderedEquals(output.getOutputs(), 2, 3, 3, 4, 4, 5);
  }
}
