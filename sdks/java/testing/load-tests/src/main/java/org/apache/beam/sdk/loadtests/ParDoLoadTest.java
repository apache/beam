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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.synthetic.SyntheticStep;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.testutils.metrics.ByteMonitor;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * Load test for {@link ParDo} operation.
 *
 * <p>The purpose of this test is to measure {@link ParDo}'s behaviour in stressful conditions. It
 * uses synthetic sources and {@link SyntheticStep} which both can be parametrized to generate keys
 * and values of various size, impose delay (sleep or cpu burnout) in various moments during the
 * pipeline execution and provide some other performance challenges.
 *
 * <p>To run it manually, use the following command:
 *
 * <pre>
 *    ./gradlew :sdks:java:testing:load-tests:run -PloadTest.args='
 *      --numberOfCounterOperations=1
 *      --sourceOptions={"numRecords":1000,...}
 *      --numberOfCounters=1
 *      --iterations=1'
 *      -PloadTest.mainClass="org.apache.beam.sdk.loadtests.ParDoLoadTest"
 * </pre>
 */
public class ParDoLoadTest extends LoadTest<ParDoLoadTest.Options> {

  private static final String METRICS_NAMESPACE = "pardo";

  /** Pipeline options specific for this test. */
  public interface Options extends LoadTestOptions {

    @Description("Number of operations on counters to be performed in one ParDo.")
    @Default.Integer(0)
    Integer getNumberOfCounterOperations();

    void setNumberOfCounterOperations(Integer count);

    @Description("Number of counters to be included in the ParDo operation")
    @Default.Integer(1)
    Integer getNumberOfCounters();

    void setNumberOfCounters(Integer count);

    @Description("Number of subsequent ParDo operations to be performed")
    Integer getIterations();

    void setIterations(Integer iterations);
  }

  private ParDoLoadTest(String[] args) throws IOException {
    super(args, Options.class, METRICS_NAMESPACE);
  }

  @Override
  protected void loadTest() {
    PCollection<KV<byte[], byte[]>> input =
        pipeline
            .apply("Read input", readFromSource(sourceOptions))
            .apply(ParDo.of(runtimeMonitor))
            .apply(ParDo.of(new ByteMonitor(METRICS_NAMESPACE, "totalBytes.count")));

    for (int i = 0; i < options.getIterations(); i++) {
      input =
          input.apply(
              String.format("Step: %d", i),
              ParDo.of(
                  new CounterOperation<>(
                      options.getNumberOfCounters(), options.getNumberOfCounterOperations())));
    }

    input.apply(ParDo.of(runtimeMonitor));
  }

  public static void main(String[] args) throws IOException {
    new ParDoLoadTest(args).run();
  }

  private static class CounterOperation<T> extends DoFn<T, T> {
    private Integer numberOfOperations;
    private List<Counter> counters = new ArrayList<>();

    CounterOperation(Integer numberOfCounters, Integer numberOfOperations) {
      for (int i = 0; i < numberOfCounters; i++) {
        counters.add(Metrics.counter("namespace", "name-" + i));
      }
      this.numberOfOperations = numberOfOperations;
    }

    @ProcessElement
    public void processElement(ProcessContext processContext) {
      for (int i = 0; i < numberOfOperations; i++) {
        for (Counter counter : counters) {
          counter.inc();
        }
      }
      processContext.output(processContext.element());
    }
  }
}
