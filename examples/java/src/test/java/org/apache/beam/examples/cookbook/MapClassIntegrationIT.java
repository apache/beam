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
package org.apache.beam.examples.cookbook;

import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.joda.time.Duration;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MapClassIntegrationIT {

  static class MapDoFn extends DoFn<KV<String, Long>, Void> {
    @StateId("mapState")
    private final StateSpec<MapState<String, Long>> mapStateSpec = StateSpecs.map();

    @ProcessElement
    public void processElement(
        @Element KV<String, Long> element, @StateId("mapState") MapState<String, Long> mapState) {
      mapState.put(Long.toString(element.getValue() % 100), element.getValue());
      if (element.getValue() % 1000 == 0) {
        Iterable<Map.Entry<String, Long>> entries = mapState.entries().read();
        if (entries != null) {
          System.err.println("ENTRIES " + Iterables.toString(entries));
        } else {
          System.err.println("ENTRIES IS NULL");
        }
      }
    }
  }

  @Test
  @Ignore("https://issues.apache.org/jira/browse/BEAM-11962")
  public void testDataflowMapState() {
    PipelineOptions options = TestPipeline.testingPipelineOptions();
    Pipeline p = Pipeline.create(options);
    p.apply(
            "GenerateSequence",
            GenerateSequence.from(0).withRate(1000, Duration.standardSeconds(1)))
        .apply("WithKeys", WithKeys.of("key"))
        .apply("MapState", ParDo.of(new MapDoFn()));
    p.run();
  }
}
