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
package org.apache.beam.sdk.extensions.euphoria.operator.test;

import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.LeftJoin;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.MapElements;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.RightJoin;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.hint.SizeHint;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;
import org.apache.beam.sdk.extensions.euphoria.operator.test.junit.AbstractOperatorTest;
import org.apache.beam.sdk.extensions.euphoria.operator.test.junit.Processing;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Collection of broadcast hash join tests.
 */
public class BroadcastHashJoinTest extends AbstractOperatorTest {

  @Processing(Processing.Type.BOUNDED)
  @Test
  public void leftBroadcastHashJoin() {
    execute(
        new JoinTest.JoinTestCase<Integer, Long, Pair<Integer, String>>() {

          @Override
          protected Dataset<Pair<Integer, String>> getOutput(
              Dataset<Integer> left, Dataset<Long> right) {
            return LeftJoin.of(
                    left, MapElements.of(right).using(i -> i).output(SizeHint.FITS_IN_MEMORY))
                .by(e -> e, e -> (int) (e % 10))
                .using(
                    (Integer l, Optional<Long> r, Collector<String> c) ->
                        c.collect(l + "+" + r.orElse(null)))
                .output();
          }

          @Override
          protected List<Integer> getLeftInput() {
            return Arrays.asList(1, 2, 3, 0, 4, 3, 2, 1);
          }

          @Override
          protected List<Long> getRightInput() {
            return Arrays.asList(11L, 12L, 13L, 14L, 15L, 11L);
          }

          @Override
          public List<Pair<Integer, String>> getUnorderedOutput() {
            return Arrays.asList(
                Pair.of(0, "0+null"),
                Pair.of(2, "2+12"),
                Pair.of(2, "2+12"),
                Pair.of(4, "4+14"),
                Pair.of(1, "1+11"),
                Pair.of(1, "1+11"),
                Pair.of(3, "3+13"),
                Pair.of(3, "3+13"),
                Pair.of(1, "1+11"),
                Pair.of(1, "1+11"));
          }
        });
  }

  @Processing(Processing.Type.BOUNDED)
  @Test
  public void rightBroadcastHashJoin() {
    execute(
        new JoinTest.JoinTestCase<Integer, Long, Pair<Integer, String>>() {

          @Override
          protected Dataset<Pair<Integer, String>> getOutput(
              Dataset<Integer> left, Dataset<Long> right) {
            return RightJoin.of(
                    MapElements.of(left).using(i -> i).output(SizeHint.FITS_IN_MEMORY), right)
                .by(e -> e, e -> (int) (e % 10))
                .using(
                    (Optional<Integer> l, Long r, Collector<String> c) ->
                        c.collect(l.orElse(null) + "+" + r))
                .output();
          }

          @Override
          protected List<Integer> getLeftInput() {
            return Arrays.asList(1, 2, 3, 0, 4, 3, 2, 1);
          }

          @Override
          protected List<Long> getRightInput() {
            return Arrays.asList(11L, 12L, 13L, 14L, 15L);
          }

          @Override
          public List<Pair<Integer, String>> getUnorderedOutput() {
            return Arrays.asList(
                Pair.of(2, "2+12"),
                Pair.of(2, "2+12"),
                Pair.of(4, "4+14"),
                Pair.of(1, "1+11"),
                Pair.of(1, "1+11"),
                Pair.of(3, "3+13"),
                Pair.of(3, "3+13"),
                Pair.of(5, "null+15"));
          }
        });
  }

  @Processing(Processing.Type.BOUNDED)
  @Test
  public void keyHashCollisionBroadcastHashJoin() {
    final String sameHashCodeKey1 = "FB";
    final String sameHashCodeKey2 = "Ea";
    execute(
        new JoinTest.JoinTestCase<String, Integer, Pair<String, String>>() {

          @Override
          protected Dataset<Pair<String, String>> getOutput(
              Dataset<String> left, Dataset<Integer> right) {
            return LeftJoin.of(
                    left, MapElements.of(right).using(i -> i).output(SizeHint.FITS_IN_MEMORY))
                .by(e -> e, e -> e % 2 == 0 ? sameHashCodeKey2 : sameHashCodeKey1)
                .using(
                    (String l, Optional<Integer> r, Collector<String> c) ->
                        c.collect(l + "+" + r.orElse(null)))
                .output();
          }

          @Override
          protected List<String> getLeftInput() {
            return Arrays.asList(sameHashCodeKey1, sameHashCodeKey2, "keyWithoutRightSide");
          }

          @Override
          protected List<Integer> getRightInput() {
            return Arrays.asList(1, 2);
          }

          @Override
          public List<Pair<String, String>> getUnorderedOutput() {
            return Arrays.asList(
                Pair.of(sameHashCodeKey1, "FB+1"),
                Pair.of(sameHashCodeKey2, "Ea+2"),
                Pair.of("keyWithoutRightSide", "keyWithoutRightSide+null"));
          }
        });
  }
}
