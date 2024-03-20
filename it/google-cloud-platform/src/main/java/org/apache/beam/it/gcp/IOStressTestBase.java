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
package org.apache.beam.it.gcp;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Instant;

/** Base class for IO Stress tests. */
public class IOStressTestBase extends IOLoadTestBase {
  /**
   * The load will initiate at 1x, progressively increase to 2x and 4x, then decrease to 2x and
   * eventually return to 1x.
   */
  protected static final int[] DEFAULT_LOAD_INCREASE_ARRAY = {1, 2, 2, 4, 2, 1};

  protected static final int DEFAULT_ROWS_PER_SECOND = 1000;

  /**
   * Generates and returns a list of LoadPeriod instances representing periods of load increase
   * based on the specified load increase array and total duration in minutes.
   *
   * @param minutesTotal The total duration in minutes for which the load periods are generated.
   * @return A list of LoadPeriod instances defining periods of load increase.
   */
  protected List<LoadPeriod> getLoadPeriods(int minutesTotal, int[] loadIncreaseArray) {

    List<LoadPeriod> loadPeriods = new ArrayList<>();
    long periodDurationMillis =
        Duration.ofMinutes(minutesTotal / loadIncreaseArray.length).toMillis();
    long startTimeMillis = 0;

    for (int loadIncreaseMultiplier : loadIncreaseArray) {
      long endTimeMillis = startTimeMillis + periodDurationMillis;
      loadPeriods.add(new LoadPeriod(loadIncreaseMultiplier, startTimeMillis, endTimeMillis));

      startTimeMillis = endTimeMillis;
    }
    return loadPeriods;
  }

  /**
   * Represents a period of time with associated load increase properties for stress testing
   * scenarios.
   */
  protected static class LoadPeriod implements Serializable {
    private final int loadIncreaseMultiplier;
    private final long periodStartMillis;
    private final long periodEndMillis;

    public LoadPeriod(int loadIncreaseMultiplier, long periodStartMillis, long periodEndMin) {
      this.loadIncreaseMultiplier = loadIncreaseMultiplier;
      this.periodStartMillis = periodStartMillis;
      this.periodEndMillis = periodEndMin;
    }

    public int getLoadIncreaseMultiplier() {
      return loadIncreaseMultiplier;
    }

    public long getPeriodStartMillis() {
      return periodStartMillis;
    }

    public long getPeriodEndMillis() {
      return periodEndMillis;
    }
  }

  /**
   * Custom Apache Beam DoFn designed for use in stress testing scenarios. It introduces a dynamic
   * load increase over time, multiplying the input elements based on the elapsed time since the
   * start of processing. This class aims to simulate various load levels during stress testing.
   */
  protected static class MultiplierDoFn<T> extends DoFn<T, T> {
    private final int startMultiplier;
    private final long startTimesMillis;
    private final List<LoadPeriod> loadPeriods;

    public MultiplierDoFn(int startMultiplier, List<LoadPeriod> loadPeriods) {
      this.startMultiplier = startMultiplier;
      this.startTimesMillis = Instant.now().getMillis();
      this.loadPeriods = loadPeriods;
    }

    @DoFn.ProcessElement
    public void processElement(
        @Element T element, OutputReceiver<T> outputReceiver, @DoFn.Timestamp Instant timestamp) {

      int multiplier = this.startMultiplier;
      long elapsedTimeMillis = timestamp.getMillis() - startTimesMillis;

      for (LoadPeriod loadPeriod : loadPeriods) {
        if (elapsedTimeMillis >= loadPeriod.getPeriodStartMillis()
            && elapsedTimeMillis < loadPeriod.getPeriodEndMillis()) {
          multiplier *= loadPeriod.getLoadIncreaseMultiplier();
          break;
        }
      }
      for (int i = 0; i < multiplier; i++) {
        outputReceiver.output(element);
      }
    }
  }
}
