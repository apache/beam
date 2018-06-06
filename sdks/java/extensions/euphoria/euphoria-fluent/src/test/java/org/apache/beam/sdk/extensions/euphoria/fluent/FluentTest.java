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
package org.apache.beam.sdk.extensions.euphoria.fluent;

import static java.util.Arrays.asList;

import java.time.Duration;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.euphoria.beam.BeamExecutor;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Count;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.ListDataSink;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.ListDataSource;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.ReduceByKey;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;

/** Test behavior of Fluent API. */
public class FluentTest {

  //TODO adapt test to Beam windowing
//  @Test
//  public void testBasics() throws Exception {
//    final Duration readDelay = Duration.ofMillis(100L);
//    ListDataSink<Set<String>> out = ListDataSink.get();
//    Fluent.flow("Test")
//        .read(
//            ListDataSource.unbounded(
//                    asList("0-one 1-two 0-three 1-four 0-five 1-six 0-seven".split(" ")))
//                .withReadDelay(readDelay))
//        // ~ create windows of size three
//        .apply(
//            input ->
//                ReduceByKey.of(input)
//                    .keyBy(e -> "")
//                    .valueBy(e -> e)
//                    .reduceBy(s -> s.collect(Collectors.toSet()))
//                    .windowBy(Count.of(3)))
//        // ~ strip the needless key and flatten out the elements thereby
//        // creating multiple elements in the output belonging to the same window
//        .flatMap(
//            (Pair<String, Set<String>> e, Collector<String> c) ->
//                e.getSecond().stream().forEachOrdered(c::collect))
//        // ~ we now expect to reconstruct the same windowing
//        // as the very initial step
//        .apply(
//            input ->
//                ReduceByKey.of(input)
//                    .keyBy(e -> "")
//                    .valueBy(e -> e)
//                    .reduceBy(s -> s.collect(Collectors.toSet())))
//        // ~ strip the needless key
//        .mapElements(Pair::getSecond)
//        .persist(out)
//        .execute(createExecutor());
//  }

  static BeamExecutor createExecutor() {
    String[] args = {"--runner=DirectRunner"};
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(PipelineOptions.class);
    return new BeamExecutor(options).withAllowedLateness(Duration.ofHours(1));
  }
}
