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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.beam.sdk.values.PCollection;

public class BatchElements<T> extends PTransform<PCollection<T>, PCollection<List<T>>> {

    public class BatchConfig {
        private int minBatchSize;
        private int maxBatchSize;
        private double targetBatchDurationSecs;
        private double targetBatchDurationSecsIncludingFixedCost;
        private double targetBatchOverhead;
        private double variance;

        // add validations to configinput
        public BatchConfig(
                int minBatchSize,
                int maxBatchSize,
                double targetBatchDurationSecs,
                double targetBatchDurationSecsIncludingFixedCost,
                double targetBatchOverhead,
                double variance) {
            this.minBatchSize = minBatchSize;
            this.maxBatchSize = maxBatchSize;
            this.targetBatchDurationSecs = targetBatchDurationSecs;
            this.targetBatchDurationSecsIncludingFixedCost = targetBatchDurationSecsIncludingFixedCost;
            this.targetBatchOverhead = targetBatchOverhead;
            this.variance = variance;
        }
    }

    public class BatchSizeEstimator {
        private List<long[]> data = new ArrayList<>();
        private BatchConfig config;
        private Integer replayLastBatchSize = null; // null = no replay pending
        private Map<Integer, Integer> batchSizeNumSeen; // tracks how many times each batch size seen
        private int warmupBatchCount = 0; //_ignore_first_n_seen_per_batch_size
        private boolean ignoreNextTiming = false;

        private static final int MAX_DATA_POINTS = 100;
        private static final int MAX_GROWTH_FACTOR = 2;

        public BatchSizeEstimator(BatchConfig config) {
            this.config = config;
            this.data = new ArrayList<>();
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
                    data.add(new long[] { batchSize, elapsed });
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
            Random random = new Random();
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
                return new double[] { ybar, 0 }; // a=ybar, b=0
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
                return new double[] { 0, ybar / xbar }; // a=0, b=avg time per element
            }

            // fit the line
            double num = 0, den = 0;
            for (int i = 0; i < n; i++) {
                num += (xs[i] - xbar) * (ys[i] - ybar);
                den += (xs[i] - xbar) * (xs[i] - xbar);
            }
            double b = num / den;
            double a = ybar - b * xbar;
            return new double[] { a, b };
        }

        private int calculateNextBatchSize() {

            // cold start
            if (config.minBatchSize == config.maxBatchSize) {
                return config.minBatchSize;
            } else if (data.size() < 1) {
                return config.minBatchSize;
            } else if (data.size() < 2) {
                // variety of regression
                return (int) Math.max(
                        Math.min(config.maxBatchSize, config.minBatchSize * MAX_GROWTH_FACTOR),
                        config.minBatchSize + 1);
            }

            // trim top 20% outliers
            List<long[]> sorted = new ArrayList<>(data);
            sorted.sort((p1, p2) -> Long.compare(p1[0], p2[0])); // sort by batch size
            int trimSize = Math.max(20, sorted.size() * 4 / 5);
            List<long[]> trimmed = sorted.subList(0, Math.min(trimSize, sorted.size()));

            // find a and b(fixed cost and per element cost)
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
                target = Math.min(target,
                        (config.targetBatchDurationSecsIncludingFixedCost - a) / b);
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
            if (seenCount <= warmupBatchCount) {
                ignoreNextTiming();
            }
            batchSizeNumSeen.put(result, seenCount);

            return result;
        }

    }

    @Override
    public PCollection<List<T>> expand(PCollection<T> input) {
        throw new UnsupportedOperationException("expand() not yet implemented");
    }
}