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
package org.apache.beam.sdk.runners;

import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.CrashingRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link PipelineRunner}.
 */
@RunWith(JUnit4.class)
public class PipelineRunnerTest {
  @Test
  public void testInstantiation() {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(CrashingRunner.class);
    PipelineRunner<?> runner = PipelineRunner.fromOptions(options);
    assertTrue(runner instanceof CrashingRunner);
  }

  private static class ScaleFn<T extends Number>
      extends SimpleFunction<T, Double> {

    private final double scalar;

    public ScaleFn(double scalar) {
      this.scalar = scalar;
    }

    @Override
    public Double apply(T input) {
      return scalar * input.doubleValue();
    }
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testRunPTransform() {
    PipelineRunner pipelineRunner = PipelineRunner.create();
    PTransform<PBegin, POutput> scaleByTwo = new PTransform<PBegin, POutput>() {
      @Override
      public POutput expand(PBegin input) {
        PCollection<Double> output = input
            .apply(Create.<Integer>of(1, 2, 3, 4))
            .apply("ScaleByTwo", MapElements.via(new ScaleFn<Integer>(2.0)));
        PAssert.that(output).containsInAnyOrder(2.0, 4.0, 6.0, 8.0);
        return output;
      }
    };
    pipelineRunner.run(scaleByTwo);
  }
}
