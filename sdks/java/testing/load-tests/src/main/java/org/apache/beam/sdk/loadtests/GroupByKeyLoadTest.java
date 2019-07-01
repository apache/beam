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
import java.util.Optional;
import org.apache.beam.sdk.io.synthetic.SyntheticStep;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.testutils.metrics.ByteMonitor;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * Load test for {@link GroupByKey} operation.
 *
 * <p>The purpose of this test is to measure {@link GroupByKey}'s behaviour in stressful conditions.
 * It uses synthetic sources and {@link SyntheticStep} which both can be parametrized to generate
 * keys and values of various size, impose delay (sleep or cpu burnout) in various moments during
 * the pipeline execution and provide some other performance challenges.
 *
 * <p>In addition, this test allows to: - fanout: produce one input (using Synthetic Source) and
 * process it with multiple sessions performing the same set of operations - reiterate produced
 * PCollection multiple times
 *
 * <p>To run it manually, use the following command:
 *
 * <pre>
 *    ./gradlew :sdks:java:testing:load-tests:run -PloadTest.args='
 *      --fanout=1
 *      --iterations=1
 *      --sourceOptions={"numRecords":1000,...}
 *      --stepOptions={"outputRecordsPerInputRecord":2...}'
 *      -PloadTest.mainClass="org.apache.beam.sdk.loadtests.GroupByKeyLoadTest"
 * </pre>
 */
public class GroupByKeyLoadTest extends LoadTest<GroupByKeyLoadTest.Options> {

  private static final String METRICS_NAMESPACE = "gbk";

  /** Pipeline options for the test. */
  public interface Options extends LoadTestOptions {

    @Description("The number of GroupByKey operations to perform in parallel (fanout)")
    @Default.Integer(1)
    Integer getFanout();

    void setFanout(Integer fanout);

    @Description("Number of reiterations over per-key-grouped values to perform.")
    @Default.Integer(1)
    Integer getIterations();

    void setIterations(Integer iterations);
  }

  private GroupByKeyLoadTest(String[] args) throws IOException {
    super(args, Options.class, METRICS_NAMESPACE);
  }

  @Override
  void loadTest() throws IOException {
    Optional<SyntheticStep> syntheticStep = createStep(options.getStepOptions());

    PCollection<KV<byte[], byte[]>> input =
        pipeline
            .apply("Read input", readFromSource(sourceOptions))
            .apply("Collect start time metrics", ParDo.of(runtimeMonitor))
            .apply(
                "Total bytes monitor",
                ParDo.of(new ByteMonitor(METRICS_NAMESPACE, "totalBytes.count")));

    input = applyWindowing(input);

    for (int branch = 0; branch < options.getFanout(); branch++) {
      applyStepIfPresent(input, format("Synthetic step (%s)", branch), syntheticStep)
          .apply(format("Group by key (%s)", branch), GroupByKey.create())
          .apply(
              format("Ungroup and reiterate (%s)", branch),
              ParDo.of(new UngroupAndReiterate(options.getIterations())))
          .apply(format("Collect end time metrics (%s)", branch), ParDo.of(runtimeMonitor));
    }
  }

  private static class UngroupAndReiterate
      extends DoFn<KV<byte[], Iterable<byte[]>>, KV<byte[], byte[]>> {

    private int iterations;

    UngroupAndReiterate(int iterations) {
      this.iterations = iterations;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      byte[] key = c.element().getKey();

      // reiterate "iterations" times, emit output only once
      for (int i = 0; i < iterations; i++) {
        for (byte[] value : c.element().getValue()) {

          if (i == iterations - 1) {
            c.output(KV.of(key, value));
          }
        }
      }
    }
  }

  public static void main(String[] args) throws IOException {
    new GroupByKeyLoadTest(args).run();
  }
}
