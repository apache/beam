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
package org.apache.beam.sdk.transforms;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowingStrategy;

public class BatchElements<T> extends PTransform<PCollection<T>, PCollection<List<T>>> {

  private final BatchConfig config;

  private BatchElements(BatchConfig config) {
    this.config = config;
  }

  public static <T> BatchElements<T> withConfig(BatchConfig config) {
    return new BatchElements<>(config);
  }

  public static final class BatchConfig implements Serializable {
    final int minBatchSize;
    final int maxBatchSize;
    final double targetBatchOverhead;
    final double targetBatchDurationSecs;
    final double targetBatchDurationSecsIncludingFixedCost;
    final double variance;

    private BatchConfig(Builder builder) {
      this.minBatchSize = builder.minBatchSize;
      this.maxBatchSize = builder.maxBatchSize;
      this.targetBatchOverhead = builder.targetBatchOverhead;
      this.targetBatchDurationSecs = builder.targetBatchDurationSecs;
      this.targetBatchDurationSecsIncludingFixedCost =
          builder.targetBatchDurationSecsIncludingFixedCost;
      this.variance = builder.variance;
    }

    public static BatchConfig defaults() {
      return BatchConfig.builder()
          .withMinBatchSize(1)
          .withMaxBatchSize(10000)
          .withTargetBatchOverhead(0.05)
          .withTargetBatchDurationSecs(10.0)
          .withVariance(0.25)
          .build();
    }

    public static Builder builder() {
      return new Builder();
    }

    public static final class Builder {
      private int minBatchSize = 1;
      private int maxBatchSize = 10_000;
      private double targetBatchOverhead = 0.05;
      private double targetBatchDurationSecs = 10.0;
      private double targetBatchDurationSecsIncludingFixedCost = -1; // -1 = unset
      private double variance = 0.25;

      private Builder() {}

      public Builder withMinBatchSize(int minBatchSize) {
        this.minBatchSize = minBatchSize;
        return this;
      }

      public Builder withMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
        return this;
      }

      public Builder withTargetBatchOverhead(double targetBatchOverhead) {
        this.targetBatchOverhead = targetBatchOverhead;
        return this;
      }

      public Builder withTargetBatchDurationSecs(double targetBatchDurationSecs) {
        this.targetBatchDurationSecs = targetBatchDurationSecs;
        return this;
      }

      public Builder withTargetBatchDurationSecsIncludingFixedCost(double value) {
        this.targetBatchDurationSecsIncludingFixedCost = value;
        return this;
      }

      public Builder withVariance(double variance) {
        this.variance = variance;
        return this;
      }

      public BatchConfig build() {
        validate();
        return new BatchConfig(this);
      }

      private void validate() {
        if (minBatchSize > maxBatchSize) {
          throw new IllegalArgumentException(
              String.format(
                  "Minimum (%d) must not be greater than maximum (%d)",
                  minBatchSize, maxBatchSize));
        }
        if (!(targetBatchOverhead > 0 && targetBatchOverhead <= 1)) {
          throw new IllegalArgumentException(
              String.format(
                  "targetBatchOverhead (%f) must be between 0 and 1", targetBatchOverhead));
        }
        if (targetBatchDurationSecs <= 0) {
          throw new IllegalArgumentException(
              String.format(
                  "targetBatchDurationSecs (%f) must be positive", targetBatchDurationSecs));
        }
        if (targetBatchDurationSecsIncludingFixedCost != -1
            && targetBatchDurationSecsIncludingFixedCost <= 0) {
          throw new IllegalArgumentException(
              String.format(
                  "targetBatchDurationSecsIncludingFixedCost (%f) must be positive",
                  targetBatchDurationSecsIncludingFixedCost));
        }
      }
    }
  }

  static class BatchSizeEstimator implements Serializable {
    private List<long[]> data = new ArrayList<>();
    private final BatchConfig config;
    private @Nullable Integer replayLastBatchSize = null; // null = no replay pending
    private final Map<Integer, Integer>
        batchSizeNumSeen; // tracks how many times each batch size seen
    private boolean ignoreNextTiming = false;
    private final Random random;

    private static final int MAX_DATA_POINTS = 100;
    private static final int MAX_GROWTH_FACTOR = 2;
    private static final int WARMUP_BATCH_COUNT = 1;

    public BatchSizeEstimator(BatchConfig config) {
      this.config = config;
      this.data = new ArrayList<>();
      this.random = new Random();
      this.batchSizeNumSeen = new HashMap<>();
    }

    public class Stopwatch implements AutoCloseable {
      private final long startTime;
      private final int batchSize;

      public Stopwatch(int batchSize) {
        this.batchSize = batchSize;
        this.startTime = System.currentTimeMillis();
      }

      @Override
      public void close() {
        long elapsed = System.currentTimeMillis() - startTime;
        if (ignoreNextTiming) {
          ignoreNextTiming = false;
          replayLastBatchSize = Math.min(batchSize, config.maxBatchSize);
        } else {
          data.add(new long[] {batchSize, elapsed});
          if (data.size() >= MAX_DATA_POINTS) {
            thinData();
          }
        }
      }
    }

    public Stopwatch recordTime(int batchSize) {
      return new Stopwatch(batchSize);
    }

    private void thinData() {
      data.remove(random.nextInt(data.size() / 4));
      data.remove(random.nextInt(data.size() / 2));
    }

    public void ignoreNextTiming() {
      this.ignoreNextTiming = true;
    }

    private double[] linearRegression(double[] xs, double[] ys) {
      int n = xs.length;
      double xbar = 0, ybar = 0;
      for (int i = 0; i < n; i++) {
        xbar += xs[i];
        ybar += ys[i];
      }
      xbar /= n;
      ybar /= n;

      if (xbar == 0) {
        return new double[] {ybar, 0}; // a=ybar, b=0
      }

      // all batch sizes identical, can't separate fixed vs per-element cost
      boolean allSame = true;
      for (double x : xs) {
        if (x != xs[0]) {
          allSame = false;
          break;
        }
      }
      if (allSame) {
        return new double[] {0, ybar / xbar}; // a=0, b=avg time per element
      }

      // fit the line
      double num = 0, den = 0;
      for (int i = 0; i < n; i++) {
        num += (xs[i] - xbar) * (ys[i] - ybar);
        den += (xs[i] - xbar) * (xs[i] - xbar);
      }
      double b = num / den;
      double a = ybar - b * xbar;
      return new double[] {a, b};
    }

    private int calculateNextBatchSize() {

      // cold start
      if (config.minBatchSize == config.maxBatchSize) {
        return config.minBatchSize;
      } else if (data.size() < 1) {
        return config.minBatchSize;
      } else if (data.size() < 2) {
        // variety of regression
        return (int)
            Math.max(
                Math.min(config.maxBatchSize, config.minBatchSize * MAX_GROWTH_FACTOR),
                config.minBatchSize + 1);
      }

      // trim top 20% outliers
      List<long[]> sorted = new ArrayList<>(data);
      sorted.sort((p1, p2) -> Long.compare(p1[0], p2[0])); // sort by batch size
      int trimSize = Math.max(20, sorted.size() * 4 / 5);
      List<long[]> trimmed = sorted.subList(0, Math.min(trimSize, sorted.size()));

      // find a and b (fixed cost and per element cost)
      double[] xs = new double[trimmed.size()];
      double[] ys = new double[trimmed.size()];
      for (int i = 0; i < trimmed.size(); i++) {
        xs[i] = trimmed.get(i)[0]; // batch size
        ys[i] = trimmed.get(i)[1]; // elapsed ms
      }
      double[] ab = linearRegression(xs, ys);
      double a = Math.max(ab[0], 1e-10); // floor at tiny value
      double b = Math.max(ab[1], 1e-20);

      // solve for target batch size
      long lastBatchSize = data.get(data.size() - 1)[0];
      int cap = (int) Math.min(lastBatchSize * MAX_GROWTH_FACTOR, config.maxBatchSize);

      double target = config.maxBatchSize;

      // 1: a + b*x = targetDurationIncludingFixedCost
      if (config.targetBatchDurationSecsIncludingFixedCost > 0) {
        target = Math.min(target, (config.targetBatchDurationSecsIncludingFixedCost - a) / b);
      }

      // 2: b*x = targetDurationSecs
      if (config.targetBatchDurationSecs > 0) {
        target = Math.min(target, config.targetBatchDurationSecs / b);
      }

      // 3: a / (a + b*x) = targetOverhead
      if (config.targetBatchOverhead > 0) {
        target = Math.min(target, (a / b) * (1.0 / config.targetBatchOverhead - 1));
      }

      // add jitter to avoid any single batch size
      int jitter = data.size() % 2;
      if (data.size() > 10) {
        target += (int) (target * config.variance * 2 * (Math.random() - 0.5));
      }

      return (int) Math.max(config.minBatchSize + jitter, Math.min(target, cap));
    }

    public int nextBatchSize() {
      int result;

      // Check if we should replay a previous batch size due to it not being recorded.
      if (replayLastBatchSize != null) {
        result = replayLastBatchSize;
        replayLastBatchSize = null;
      } else {
        result = calculateNextBatchSize();
      }

      // track how many times we've seen this batch size
      int seenCount = batchSizeNumSeen.getOrDefault(result, 0) + 1;
      if (seenCount <= WARMUP_BATCH_COUNT) {
        ignoreNextTiming();
      }
      batchSizeNumSeen.put(result, seenCount);

      return result;
    }
  }

  @SuppressWarnings("initialization")
  static class GlobalWindowsBatchingDoFn<T> extends DoFn<T, List<T>> {
    private transient BatchSizeEstimator estimator;
    private final BatchConfig config;
    private List<T> batch;
    private int runningBatchSize;
    private int targetBatchSize;

    public GlobalWindowsBatchingDoFn(BatchConfig config) {
      this.config = config;
    }

    @Setup
    public void setup() {
      estimator = new BatchSizeEstimator(config);
    }

    @StartBundle
    public void startBundle() {
      batch = new ArrayList<>();
      runningBatchSize = 0;
      targetBatchSize = estimator.nextBatchSize();
    }

    @ProcessElement
    public void processElement(@Element T element, OutputReceiver<List<T>> receiver) {
      int elementSize = 1;
      if (runningBatchSize + elementSize > targetBatchSize) {
        if (runningBatchSize > 0 && !batch.isEmpty()) {
          try (BatchElements.BatchSizeEstimator.Stopwatch sw =
              estimator.recordTime(runningBatchSize)) {
            receiver.output(batch); // emit full batch downstream
          }
        }
        batch = new ArrayList<>();
        runningBatchSize = 0;
        targetBatchSize = estimator.nextBatchSize();
      }
      batch.add(element);
      runningBatchSize += elementSize;
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext context) {
      if (!batch.isEmpty()) {
        try (BatchElements.BatchSizeEstimator.Stopwatch sw =
            estimator.recordTime(runningBatchSize)) {
          context.output( // flush leftover elements
              batch,
              GlobalWindow.INSTANCE.maxTimestamp(), // end of window timestamp
              GlobalWindow.INSTANCE // global window
              );
        }
      }
    }
  }

  @SuppressWarnings("initialization")
  static class WindowAwareBatchingDoFn<T> extends DoFn<T, List<T>> {
    private transient BatchSizeEstimator estimator;
    private final BatchConfig config;
    private Map<BoundedWindow, SizedBatch<T>> batches;
    private int targetBatchSize;

    private static final int MAX_LIVE_WINDOWS = 10;

    private static class SizedBatch<T> {
      List<T> elements = new ArrayList<>();
      int size = 0;
    }

    private WindowAwareBatchingDoFn(BatchConfig config) {
      this.config = config;
    }

    @Setup
    public void setup() {
      estimator = new BatchSizeEstimator(config);
    }

    @StartBundle
    public void startBundle() {
      batches = new HashMap<>();
      targetBatchSize = estimator.nextBatchSize();
    }

    @ProcessElement
    public void processElement(
        @Element T element, BoundedWindow window, OutputReceiver<List<T>> receiver) {

      // get or create batch for this window
      SizedBatch<T> batch = batches.computeIfAbsent(window, w -> new SizedBatch<>());

      int elementSize = 1;

      // emit if this window's batch is full
      if (batch.size + elementSize > targetBatchSize) {
        try (BatchSizeEstimator.Stopwatch sw = estimator.recordTime(batch.size)) {
          receiver.output(batch.elements);
        }
        batches.remove(window);
        targetBatchSize = estimator.nextBatchSize();
        // create fresh batch for this window after emit
        batch = batches.computeIfAbsent(window, w -> new SizedBatch<>());
      }

      batch.elements.add(element);
      batch.size += elementSize;

      // evict largest window if too many live windows
      if (batches.size() > MAX_LIVE_WINDOWS) {
        Map.Entry<BoundedWindow, SizedBatch<T>> largest =
            batches.entrySet().stream().max(Comparator.comparingInt(e -> e.getValue().size)).get();

        try (BatchSizeEstimator.Stopwatch sw = estimator.recordTime(largest.getValue().size)) {
          receiver.output(largest.getValue().elements);
        }
        batches.remove(largest.getKey());
        targetBatchSize = estimator.nextBatchSize();
      }
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext context) {
      for (Map.Entry<BoundedWindow, SizedBatch<T>> entry : batches.entrySet()) {
        BoundedWindow window = entry.getKey();
        SizedBatch<T> batch = entry.getValue();
        if (!batch.elements.isEmpty()) {
          try (BatchSizeEstimator.Stopwatch sw = estimator.recordTime(batch.size)) {
            context.output(batch.elements, window.maxTimestamp(), window);
          }
        }
      }
    }
  }

  @Override
  public PCollection<List<T>> expand(PCollection<T> input) {
    if (input.getWindowingStrategy().equals(WindowingStrategy.globalDefault())) {
      return input.apply(ParDo.of(new GlobalWindowsBatchingDoFn<>(config)));
    } else {
      return input.apply(ParDo.of(new WindowAwareBatchingDoFn<>(config)));
    }
  }
}
