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
package org.apache.beam.sdk.loadtests;

import static java.lang.String.format;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Optional;
import org.apache.beam.sdk.io.synthetic.SyntheticStep;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.testutils.metrics.ByteMonitor;
import org.apache.beam.sdk.testutils.metrics.TimeMonitor;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;

/**
 * Load test for {@link ParDo} operation.
 *
 * <p>The purpose of this test is to measure {@link Combine}'s behaviour in stressful conditions. It
 * uses synthetic sources and {@link SyntheticStep} which both can be parametrized to generate keys
 * and values of various size, impose delay (sleep or cpu burnout) in various moments during the
 * pipeline execution and provide some other performance challenges.
 *
 * <p>You can choose between multiple combine modes to test per key combine operations ({@link
 * CombinerType}).
 *
 * <p>To run it manually, use the following command:
 *
 * <pre>
 *    ./gradlew :sdks:java:testing:load-tests:run -PloadTest.args='
 *      --fanout=1
 *      --perKeyCombinerType=TOP_LARGEST
 *      --topCount=10
 *      --sourceOptions={"numRecords":1000,...}
 *      --stepOptions={"outputRecordsPerInputRecord":2...}'
 *      -PloadTest.mainClass="org.apache.beam.sdk.loadtests.CombineLoadTest"
 * </pre>
 */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public class CombineLoadTest extends LoadTest<CombineLoadTest.Options> {

  private static final String METRICS_NAMESPACE = "combine";

  /** Enumerates per-key combiners available in the test. */
  public enum CombinerType {
    TOP_LARGEST,
    MEAN,
    SUM,
    COUNT
  }

  /** Pipeline options specific for this test. */
  public interface Options extends LoadTestOptions {

    @Description("Number consequent of ParDo operations (SyntheticSteps) to be performed.")
    @Default.Integer(1)
    Integer getNumberOfCounterOperations();

    void setNumberOfCounterOperations(Integer count);

    @Description("The number of Combine operations to perform in parallel.")
    @Default.Integer(1)
    Integer getFanout();

    void setFanout(Integer fanout);

    @Description("Per key combiner type.")
    @Default.Enum("MEAN")
    CombinerType getPerKeyCombiner();

    void setPerKeyCombiner(CombinerType combinerType);

    @Description("Number of top results to combine (if applicable).")
    Integer getTopCount();

    void setTopCount(Integer topCount);

    @Description("Number of reiterations over the values to perform.")
    @Default.Integer(1)
    Integer getIterations();

    void setIterations(Integer iterations);
  }

  private CombineLoadTest(String[] args) throws IOException {
    super(args, Options.class, METRICS_NAMESPACE);
  }

  @Override
  protected void loadTest() throws IOException {
    Optional<SyntheticStep> syntheticStep = createStep(options.getStepOptions());

    PCollection<KV<byte[], byte[]>> input =
        pipeline
            .apply("Read input", readFromSource(sourceOptions))
            .apply(
                "Collect start time metric",
                ParDo.of(new TimeMonitor<>(METRICS_NAMESPACE, "runtime")))
            .apply(
                "Collect metrics",
                ParDo.of(new ByteMonitor(METRICS_NAMESPACE, "totalBytes.count")));

    input = applyWindowing(input);

    for (int i = 0; i < options.getFanout(); i++) {
      applyStepIfPresent(input, format("Step: %d", i), syntheticStep)
          .apply(format("Convert to Long: %d", i), MapElements.via(new ByteValueToLong()))
          .apply(format("Combine: %d", i), getPerKeyCombiner(options.getPerKeyCombiner()))
          .apply(
              "Collect end time metric", ParDo.of(new TimeMonitor<>(METRICS_NAMESPACE, "runtime")));
    }
  }

  public PTransform<PCollection<KV<byte[], Long>>, ? extends PCollection> getPerKeyCombiner(
      CombinerType combinerType) {
    switch (combinerType) {
      case MEAN:
        return Mean.perKey();
      case TOP_LARGEST:
        Preconditions.checkArgument(
            options.getTopCount() != null,
            "You should set \"--topCount\" option to use TOP combiners.");
        return Top.largestPerKey(options.getTopCount());
      case SUM:
        return Sum.longsPerKey();
      case COUNT:
        return Count.perKey();
      default:
        throw new IllegalArgumentException("No such combiner!");
    }
  }

  private static class ByteValueToLong
      extends SimpleFunction<KV<byte[], byte[]>, KV<byte[], Long>> {

    @Override
    public KV<byte[], Long> apply(KV<byte[], byte[]> input) {
      return KV.of(input.getKey(), new BigInteger(input.getValue()).longValue());
    }
  }

  public static void main(String[] args) throws IOException {
    new CombineLoadTest(args).run();
  }
}
