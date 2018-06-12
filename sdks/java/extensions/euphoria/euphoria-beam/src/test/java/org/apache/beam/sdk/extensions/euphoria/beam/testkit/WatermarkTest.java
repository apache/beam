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
package org.apache.beam.sdk.extensions.euphoria.beam.testkit;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.extensions.euphoria.beam.testkit.junit.AbstractOperatorTest;
import org.apache.beam.sdk.extensions.euphoria.beam.testkit.junit.Processing;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.TimeInterval;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.AssignEventTime;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Join;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.MapElements;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Triple;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.junit.Test;

/**
 * TODO: add javadoc.
 */
public class WatermarkTest extends AbstractOperatorTest {

  // ~ see https://github.com/seznam/euphoria/issues/119
  @Processing(Processing.Type.UNBOUNDED)
  @Test
  public void joinOnFastAndSlowInputs() {
    execute(
        new JoinTest.JoinTestCase<
            Pair<String, Long>, Pair<String, Long>, Triple<TimeInterval, String, String>>() {

          // ~ a very fast source
          @Override
          protected List<Pair<String, Long>> getLeftInput() {
            return Arrays.asList(Pair.of("fi", 1L), Pair.of("fa", 2L));
          }

          // ~ a very slow source
          @Override
          protected List<Pair<String, Long>> getRightInput() {
            // TODO: speed is undefined here!
            return Arrays.asList(Pair.of("ha", 1L), Pair.of("ho", 4L));
          }

          @Override
          protected Dataset<Triple<TimeInterval, String, String>> getOutput(
              Dataset<Pair<String, Long>> left, Dataset<Pair<String, Long>> right) {
            left = AssignEventTime.of(left).using(Pair::getSecond).output();
            right = AssignEventTime.of(right).using(Pair::getSecond).output();
            Dataset<Pair<String, Triple<TimeInterval, String, String>>> joined =
                Join.of(left, right)
                    .by(p -> "", p -> "")
                    .using(
                        (Pair<String, Long> l,
                            Pair<String, Long> r,
                            Collector<Triple<TimeInterval, String, String>> c) ->
                            c.collect(
                                Triple.of(
                                    (TimeInterval) c.getWindow(), l.getFirst(), r.getFirst())))
                    .windowBy(FixedWindows.of(org.joda.time.Duration.millis(10)))
                    .triggeredBy(DefaultTrigger.of())
                    .discardingFiredPanes()
                    .output();
            return MapElements.of(joined).using(Pair::getSecond).output();
          }

          @Override
          public List<Triple<TimeInterval, String, String>> getUnorderedOutput() {
            TimeInterval expectedWindow = new TimeInterval(0, 10);
            return Arrays.asList(
                Triple.of(expectedWindow, "fi", "ha"),
                Triple.of(expectedWindow, "fi", "ho"),
                Triple.of(expectedWindow, "fa", "ha"),
                Triple.of(expectedWindow, "fa", "ho"));
          }
        });
  }
}
