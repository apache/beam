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
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.model.Statement;

/**
 * Tests for the {@link TestPipelineRule} class.
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

  private abstract class SerializableStatement extends Statement implements Serializable {

  }

  private static final List<String> WORDS = Collections.singletonList("hi there");
  private static final String EXPECTED = "expected";

  @Rule
  public transient TestPipelineRule pipeline = initPipelineRule();

  @Rule
  public transient ExpectedException expectedException = ExpectedException.none();

  private TestPipelineRule initPipelineRule() {
    final PipelineOptions pipelineOptions = TestPipeline.testingPipelineOptions();
    pipelineOptions.setRunner(DummyRunner.class);
    return new TestPipelineRule(pipelineOptions);
  }

  private PCollection<String> pCollection() {
    return pipeline
        .apply(Create.of(WORDS).withCoder(StringUtf8Coder.of()))
        .apply(MapElements.via(new SimpleFunction<String, String>() {

          @Override
          public String apply(final String input) {
            return EXPECTED;
          }
        }));
  }

  private Statement normalFlow() throws Exception {
    return new SerializableStatement() {

      @Override
      public void evaluate() throws Throwable {
        PAssert.that(pCollection()).containsInAnyOrder(EXPECTED);
        pipeline.run().waitUntilFinish();
      }
    };
  }

  private Statement pipelineRunMissing() throws Exception {
    return new SerializableStatement() {

      @Override
      public void evaluate() throws Throwable {
        PAssert.that(pCollection()).containsInAnyOrder(EXPECTED);
      }
    };
  }

  private Statement pipelineRunBeforePAssert() throws Exception {
    return new SerializableStatement() {

      @Override
      public void evaluate() throws Throwable {
        pipeline.run().waitUntilFinish();
        PAssert.that(pCollection()).containsInAnyOrder(EXPECTED);
      }
    };
  }

  private void runTestCase(final Statement statement) throws Throwable {
    final TestRule rule = pipeline;
    rule.apply(statement, null).evaluate();
  }

  @Test
  public void testPipelineRunMissing() throws Throwable {
    expectedException.expect(TestPipelineRule.PipelineRunMissingException.class);
    runTestCase(pipelineRunMissing());
  }

  @Test
  public void testPipelineRunBeforePAssert() throws Throwable {
    expectedException.expect(TestPipelineRule.PipelineRunBeforePAssertException.class);
    runTestCase(pipelineRunBeforePAssert());
  }

  @Test
  public void testNormalFlow() throws Throwable {
    runTestCase(normalFlow());
  }
}
