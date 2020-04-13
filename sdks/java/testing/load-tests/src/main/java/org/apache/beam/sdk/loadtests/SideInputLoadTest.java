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
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import org.apache.beam.sdk.io.synthetic.SyntheticStep;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.testutils.metrics.ByteMonitor;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Load test for operations involving side inputs.
 *
 * <p>The purpose of this test is to measure cost of materialization or lookup of side inputs. It
 * uses synthetic sources and {@link SyntheticStep} which can be parametrized to generate records
 * with various sizes of keys and values, impose delays in the pipeline and simulate other
 * performance challenges.
 *
 * <p>To run the test manually, use the following command:
 *
 * <pre>
 *   ./gradlew :sdks:java:testing:load-tests:run -PloadTest.args='
 *    --sourceOptions={"numRecords":2000, ...}
 *    --sideInputType=ITERABLE
 *    --accessPercentage=1
 *    --windowCount=200
 * </pre>
 */
public class SideInputLoadTest extends LoadTest<SideInputLoadTest.Options> {

  private static final String METRICS_NAMESPACE = "sideinput";
  private static final Instant TIME = new Instant();

  public SideInputLoadTest(String[] args) throws IOException {
    super(args, Options.class, METRICS_NAMESPACE);
  }

  @Override
  void loadTest() throws IOException {
    Optional<SyntheticStep> syntheticStep = createStep(options.getStepOptions());
    PCollection<KV<byte[], byte[]>> input =
        pipeline
            .apply(readFromSource(sourceOptions))
            .apply(ParDo.of(new AddTimestamps()))
            .apply("Collect start time metrics", ParDo.of(runtimeMonitor))
            .apply(ParDo.of(new ByteMonitor(METRICS_NAMESPACE, "totalBytes.count")));

    performTestWithSideInput(
        input, SideInputMaterializationType.valueOf(options.getSideInputType()), syntheticStep);
  }

  private void performTestWithSideInput(
      PCollection<KV<byte[], byte[]>> input,
      SideInputMaterializationType sideInputType,
      Optional<SyntheticStep> syntheticStep) {
    switch (sideInputType) {
      case ITERABLE:
        performTestWithIterable(input, syntheticStep);
        break;
      case MAP:
        performTestWithMap(input, syntheticStep);
        break;
      case LIST:
        performTestWithList(input, syntheticStep);
        break;
    }
  }

  private void performTestWithList(
      PCollection<KV<byte[], byte[]>> input, Optional<SyntheticStep> syntheticStep) {
    applyStepIfPresent(input, "Synthetic step", syntheticStep);
    PCollectionView<List<KV<byte[], byte[]>>> sideInput =
        applyWindowingIfPresent(input).apply(View.asList());
    input
        .apply(ParDo.of(new SideInputTestWithList(sideInput)).withSideInputs(sideInput))
        .apply("Collect end time metrics", ParDo.of(runtimeMonitor));
  }

  private void performTestWithMap(
      PCollection<KV<byte[], byte[]>> input, Optional<SyntheticStep> syntheticStep) {
    applyStepIfPresent(input, "Synthetic step", syntheticStep);
    PCollectionView<Map<byte[], byte[]>> sideInput =
        applyWindowingIfPresent(input).apply(View.asMap());
    PCollectionView<List<byte[]>> randomKeys =
        pipeline
            .apply(Create.of(0))
            .apply(
                ParDo.of(new GetRandomKeyList(sideInput, options.getAccessPercentage()))
                    .withSideInputs(sideInput))
            .apply(Flatten.iterables())
            .apply(View.asList());

    input
        .apply(
            ParDo.of(new SideInputTestWithMap(sideInput, randomKeys))
                .withSideInputs(sideInput, randomKeys))
        .apply("Collect end time metrics", ParDo.of(runtimeMonitor));
  }

  private void performTestWithIterable(
      PCollection<KV<byte[], byte[]>> input, Optional<SyntheticStep> syntheticStep) {
    applyStepIfPresent(input, "Synthetic step", syntheticStep);
    PCollectionView<Iterable<KV<byte[], byte[]>>> sideInput;
    sideInput = applyWindowingIfPresent(input).apply(View.asIterable());
    input
        .apply(ParDo.of(new SideInputTestWithIterable(sideInput)).withSideInputs(sideInput))
        .apply("Collect end time metrics", ParDo.of(runtimeMonitor));
  }

  private PCollection<KV<byte[], byte[]>> applyWindowingIfPresent(
      PCollection<KV<byte[], byte[]>> input) {
    PCollection<KV<byte[], byte[]>> windowedInput = input;
    if (options.getWindowCount() != 1) {
      long windowDurationMilis = sourceOptions.numRecords / options.getWindowCount();
      windowedInput =
          input.apply(Window.into(FixedWindows.of(Duration.millis(windowDurationMilis))));
    }
    return windowedInput;
  }

  private static class AddTimestamps extends DoFn<KV<byte[], byte[]>, KV<byte[], byte[]>> {
    private static Instant timestamp = TIME;

    @ProcessElement
    public void processElement(ProcessContext c) {
      timestamp = TIME.plus(1L);
      c.outputWithTimestamp(c.element(), timestamp);
    }
  }

  private static class SideInputTestWithList extends DoFn<KV<byte[], byte[]>, KV<byte[], byte[]>> {

    private final PCollectionView<List<KV<byte[], byte[]>>> sideInput;

    public SideInputTestWithList(PCollectionView<List<KV<byte[], byte[]>>> sideInput) {
      this.sideInput = sideInput;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      List<KV<byte[], byte[]>> si = c.sideInput(sideInput);

      for (KV<byte[], byte[]> sideInputElement : si) {
        // for every _input_ element iterate over all _sideInput_ elements
        // count consumed bytes, examine memory usage, etc (Metrics API).
        byte[] key = sideInputElement.getKey();
      }
    }
  }

  private static class SideInputTestWithMap extends DoFn<KV<byte[], byte[]>, KV<byte[], byte[]>> {

    private final PCollectionView<Map<byte[], byte[]>> sideInput;
    private final PCollectionView<List<byte[]>> randomKeyList;

    public SideInputTestWithMap(
        PCollectionView<Map<byte[], byte[]>> sideInput,
        PCollectionView<List<byte[]>> randomKeyList) {
      this.sideInput = sideInput;
      this.randomKeyList = randomKeyList;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Map<byte[], byte[]> si = c.sideInput(sideInput);
      List<byte[]> keys = c.sideInput(randomKeyList);
      for (byte[] key : keys) {
        byte[] value = si.get(key);
      }
    }
  }

  private static class GetRandomKeyList extends DoFn<Integer, List<byte[]>> {

    private final int keyPercentage;
    private PCollectionView<Map<byte[], byte[]>> sideInput;

    public GetRandomKeyList(PCollectionView<Map<byte[], byte[]>> sideInput, int keyPercentage) {
      this.sideInput = sideInput;
      this.keyPercentage = keyPercentage;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Map<byte[], byte[]> kvs = c.sideInput(sideInput);
      ArrayList<byte[]> keySet = new ArrayList<>(kvs.keySet());
      int keyCount = keySet.size() * keyPercentage / 100;
      Random r = new Random();
      int limit = keySet.size();
      ArrayList<byte[]> chosenKeys = new ArrayList<>();
      for (int i = 0; i < keyCount; i++) {
        chosenKeys.add(keySet.get(r.nextInt(limit)));
      }
      c.output(chosenKeys);
    }
  }

  private class SideInputTestWithIterable extends DoFn<KV<byte[], byte[]>, KV<byte[], byte[]>> {

    private final PCollectionView<Iterable<KV<byte[], byte[]>>> sideInput;

    public SideInputTestWithIterable(PCollectionView<Iterable<KV<byte[], byte[]>>> sideInput) {
      this.sideInput = sideInput;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Iterable<KV<byte[], byte[]>> si = c.sideInput(sideInput);
      Integer accessPercentage = options.getAccessPercentage();
      int elementCount = (int) (sourceOptions.numRecords * accessPercentage / 100);
      for (KV<byte[], byte[]> sideInputElement : si) {
        if (--elementCount < 0) {
          break;
        }
        // for every _input_ element iterate over all _sideInput_ elements
        // count consumed bytes, examine memory usage, etc (Metrics API).
        byte[] key = sideInputElement.getKey();
      }
    }
  }

  public enum SideInputMaterializationType {
    ITERABLE,
    MAP,
    LIST;
  }

  public interface Options extends LoadTestOptions {

    @Description("Side input type")
    @Validation.Required
    String getSideInputType();

    void setSideInputType(String value);

    @Description("Percentage of records to be accessed")
    @Default.Integer(100)
    Integer getAccessPercentage();

    void setAccessPercentage(Integer value);

    @Description("Number of windows")
    @Default.Integer(1)
    Integer getWindowCount();

    void setWindowCount(Integer value);
  }

  public static void main(String[] args) throws IOException {
    new SideInputLoadTest(args).run();
  }
}
