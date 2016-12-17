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
package org.apache.beam.sdk.testing;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.AggregatorRetrievalException;
import org.apache.beam.sdk.AggregatorValues;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for the {@link TestPipeline} as a JUnit rule.
 */
@RunWith(JUnit4.class)
public class TestPipelineRuleTest implements Serializable {

  private static class DummyRunner extends PipelineRunner<PipelineResult> {

    @SuppressWarnings("unused") // used by reflection
    public static DummyRunner fromOptions(final PipelineOptions opts) {
      return new DummyRunner();
    }

    @Override
    public PipelineResult run(final Pipeline pipeline) {
      return new PipelineResult() {

        @Override
        public State getState() {
          return null;
        }

        @Override
        public State cancel() throws IOException {
          return null;
        }

        @Override
        public State waitUntilFinish(final Duration duration) {
          return null;
        }

        @Override
        public State waitUntilFinish() {
          return null;
        }

        @Override
        public <T> AggregatorValues<T> getAggregatorValues(final Aggregator<?, T> aggregator)
            throws AggregatorRetrievalException {
          return null;
        }

        @Override
        public MetricResults metrics() {
          return null;
        }
      };
    }
  }

  private static final List<String> WORDS = Collections.singletonList("hi there");
  private static final String DUMMY = "expected";

  private final transient TestPipeline pipeline =
      TestPipeline.fromOptions(pipelineOptions()).enableAbandonedNodeEnforcement(true);

  private final transient ExpectedException exception = ExpectedException.none();

  @Rule
  public transient RuleChain ruleOrder = RuleChain.outerRule(exception).around(pipeline);

  private static PipelineOptions pipelineOptions() {
    final PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
    pipelineOptions.setRunner(DummyRunner.class);
    return pipelineOptions;
  }

  private PCollection<String> pCollection() {
    return
        addTransform(pipeline
                         .apply(Create.of(WORDS)
                                      .withCoder(StringUtf8Coder.of())));
  }

  private PCollection<String> addTransform(final PCollection<String> pCollection) {
    return
        pCollection
            .apply(MapElements.via(new SimpleFunction<String, String>() {

              @Override
              public String apply(final String input) {
                return DUMMY;
              }
            }));
  }

  @Test
  public void testPipelineRunMissing() throws Throwable {
    exception.expect(TestPipeline.PipelineRunMissingException.class);
    PAssert.that(pCollection()).containsInAnyOrder(DUMMY);
    // missing pipeline#run
  }

  @Test
  public void testPipelineHasAbandonedPAssertNode() throws Throwable {
    exception.expect(TestPipeline.AbandonedNodeException.class);
    exception.expectMessage("PAssert");

    final PCollection<String> pCollection = pCollection();
    PAssert.that(pCollection).containsInAnyOrder(DUMMY);
    pipeline.run().waitUntilFinish();

    // dangling PAssert
    PAssert.that(pCollection).containsInAnyOrder(DUMMY);
  }

  @Test
  public void testPipelineHasAbandonedPTransformNode() throws Throwable {
    exception.expect(TestPipeline.AbandonedNodeException.class);
    exception.expectMessage("PTransform");

    final PCollection<String> pCollection = pCollection();
    PAssert.that(pCollection).containsInAnyOrder(DUMMY);
    pipeline.run().waitUntilFinish();

    // dangling PTransform
    addTransform(pCollection);
  }

  @Test
  public void testNormalFlowWithPAssert() throws Throwable {
    PAssert.that(pCollection()).containsInAnyOrder(DUMMY);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testAutoAddMissingRunFlow() throws Throwable {
    PAssert.that(pCollection()).containsInAnyOrder(DUMMY);
    // missing pipeline#run, but have it auto-added.
    pipeline.enableAutoRunIfMissing(true);
  }

  @Test
  public void testDisableStrictPAssertFlow() throws Throwable {
    pCollection();
    // dangling PTransform, but ignore it
    pipeline.enableAbandonedNodeEnforcement(false);
  }
}
