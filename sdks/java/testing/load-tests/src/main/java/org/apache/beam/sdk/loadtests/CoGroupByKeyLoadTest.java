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

import static org.apache.beam.sdk.loadtests.SyntheticUtils.applyStepIfPresent;
import static org.apache.beam.sdk.loadtests.SyntheticUtils.createStep;
import static org.apache.beam.sdk.loadtests.SyntheticUtils.fromJsonString;

import java.io.IOException;
import java.util.Optional;
import org.apache.beam.sdk.io.synthetic.SyntheticBoundedIO;
import org.apache.beam.sdk.io.synthetic.SyntheticBoundedIO.SyntheticSourceOptions;
import org.apache.beam.sdk.io.synthetic.SyntheticStep;
import org.apache.beam.sdk.loadtests.metrics.MetricsMonitor;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Load test for {@link CoGroupByKey} operation.
 *
 * <p>The purpose of this test is to measure {@link CoGroupByKey}'s behaviour in stressful
 * conditions. It uses {@link SyntheticBoundedIO} and {@link SyntheticStep} which both can be
 * parametrized to generate keys and values of various size, impose delay (sleep or cpu burnout) in
 * various moments during the pipeline execution and provide some other performance challenges.
 *
 * <p>In addition, this test allows to reiterate produced PCollection multiple times to see how the
 * pipeline behaves (e.g. if caches work etc.).
 *
 * @see SyntheticStep
 * @see SyntheticBoundedIO
 *     <p>To run it manually, use the following command:
 *     <pre>
 *    ./gradlew :beam-sdks-java-load-tests:run -PloadTest.args='
 *      --iterations=1
 *      --sourceOptions={"numRecords":1000,...}
 *      --coSourceOptions={"numRecords":1000,...}
 *      --stepOptions={"outputRecordsPerInputRecord":2...}'
 *      -PloadTest.mainClass="org.apache.beam.sdk.loadtests.CoGroupByKeyLoadTest"
 * </pre>
 */
public class CoGroupByKeyLoadTest extends LoadTest<CoGroupByKeyLoadTest.Options> {

  private static final TupleTag<byte[]> INPUT_TAG = new TupleTag<>("input");
  private static final TupleTag<byte[]> CO_INPUT_TAG = new TupleTag<>("co-input");
  private static final String METRICS_NAMESPACE = "co_gbk";

  /** Pipeline options specific for this test. */
  public interface Options extends LoadTestOptions {

    @Description("Options for synthetic co-source.")
    @Validation.Required
    String getCoSourceOptions();

    void setCoSourceOptions(String sourceOptions);

    @Description("Number of reiterations over per-key-grouped values to perform.")
    @Default.Integer(1)
    Integer getIterations();

    void setIterations(Integer iterations);
  }

  private CoGroupByKeyLoadTest(String[] args) throws IOException {
    super(args, Options.class, METRICS_NAMESPACE);
  }

  @Override
  void loadTest() throws IOException {
    SyntheticSourceOptions coSourceOptions =
        fromJsonString(options.getCoSourceOptions(), SyntheticSourceOptions.class);

    Optional<SyntheticStep> syntheticStep = createStep(options.getStepOptions());

    PCollection<KV<byte[], byte[]>> input =
        applyStepIfPresent(
            pipeline.apply("Read input", SyntheticBoundedIO.readFrom(sourceOptions)),
            "Synthetic step for input",
            syntheticStep);

    PCollection<KV<byte[], byte[]>> coInput =
        applyStepIfPresent(
            pipeline.apply("Read co-input", SyntheticBoundedIO.readFrom(coSourceOptions)),
            "Synthetic step for co-input",
            syntheticStep);

    KeyedPCollectionTuple.of(INPUT_TAG, input)
        .and(CO_INPUT_TAG, coInput)
        .apply("CoGroupByKey", CoGroupByKey.create())
        .apply("Ungroup and reiterate", ParDo.of(new UngroupAndReiterate(options.getIterations())))
        .apply("Collect metrics", ParDo.of(new MetricsMonitor(METRICS_NAMESPACE)));
  }

  private static class UngroupAndReiterate
      extends DoFn<KV<byte[], CoGbkResult>, KV<byte[], byte[]>> {

    private int iterations;

    UngroupAndReiterate(int iterations) {
      this.iterations = iterations;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      byte[] key = c.element().getKey();
      CoGbkResult elementValue = c.element().getValue();

      Iterable<byte[]> inputs = elementValue.getAll(INPUT_TAG);
      Iterable<byte[]> coInputs = elementValue.getAll(CO_INPUT_TAG);

      // Reiterate "iterations" times, emit output only once.
      for (int i = 0; i < iterations; i++) {
        for (byte[] value : inputs) {
          if (i == iterations - 1) {
            c.output(KV.of(key, value));
          }
        }

        for (byte[] value : coInputs) {
          if (i == iterations - 1) {
            c.output(KV.of(key, value));
          }
        }
      }
    }
  }

  public static void main(String[] args) throws IOException {
    new CoGroupByKeyLoadTest(args).run();
  }
}
