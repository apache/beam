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
package org.apache.beam.sdk.extensions.euphoria.core.testkit;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Join;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.LeftJoin;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.MapElements;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.RightJoin;
import org.apache.beam.sdk.extensions.euphoria.core.translate.BroadcastHashJoinTranslator;
import org.apache.beam.sdk.extensions.euphoria.core.translate.EuphoriaOptions;
import org.apache.beam.sdk.extensions.euphoria.core.translate.provider.CompositeProvider;
import org.apache.beam.sdk.extensions.euphoria.core.translate.provider.GenericTranslatorProvider;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Collection of broadcast hash join tests. */
@RunWith(JUnit4.class)
public class BroadcastHashJoinTest extends AbstractOperatorTest {

  private abstract static class TestCase<LeftT, RightT, OutputT>
      extends JoinTest.JoinTestCase<LeftT, RightT, OutputT> {

    @Override
    public PCollection<OutputT> getOutput(Pipeline pipeline) {
      pipeline
          .getOptions()
          .as(EuphoriaOptions.class)
          .setTranslatorProvider(
              CompositeProvider.of(
                  GenericTranslatorProvider.newBuilder()
                      .register(
                          Join.class,
                          (Join op) -> {
                            String name = ((Optional<String>) op.getName()).orElse("");
                            return name.toLowerCase().startsWith("broadcast");
                          },
                          new BroadcastHashJoinTranslator<>())
                      .build(),
                  GenericTranslatorProvider.createWithDefaultTranslators()));

      return super.getOutput(pipeline);
    }
  }

  @Test
  public void leftBroadcastHashJoin() {
    execute(
        new TestCase<Integer, Long, KV<Integer, String>>() {

          @Override
          protected PCollection<KV<Integer, String>> getOutput(
              PCollection<Integer> left, PCollection<Long> right) {
            return LeftJoin.named("broadcast-leftJoin")
                .of(left, MapElements.of(right).using(i -> i).output())
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
          protected TypeDescriptor<Integer> getLeftInputType() {
            return TypeDescriptors.integers();
          }

          @Override
          protected TypeDescriptor<Long> getRightInputType() {
            return TypeDescriptors.longs();
          }

          @Override
          protected List<Long> getRightInput() {
            return Arrays.asList(11L, 12L, 13L, 14L, 15L, 11L);
          }

          @Override
          public List<KV<Integer, String>> getUnorderedOutput() {
            return Arrays.asList(
                KV.of(0, "0+null"),
                KV.of(2, "2+12"),
                KV.of(2, "2+12"),
                KV.of(4, "4+14"),
                KV.of(1, "1+11"),
                KV.of(1, "1+11"),
                KV.of(3, "3+13"),
                KV.of(3, "3+13"),
                KV.of(1, "1+11"),
                KV.of(1, "1+11"));
          }
        });
  }

  @Test
  public void rightBroadcastHashJoin() {
    execute(
        new TestCase<Integer, Long, KV<Integer, String>>() {

          @Override
          protected PCollection<KV<Integer, String>> getOutput(
              PCollection<Integer> left, PCollection<Long> right) {
            return RightJoin.named("BroadcastRightJoin")
                .of(MapElements.of(left).using(i -> i).output(), right)
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
          protected TypeDescriptor<Integer> getLeftInputType() {
            return TypeDescriptors.integers();
          }

          @Override
          protected List<Long> getRightInput() {
            return Arrays.asList(11L, 12L, 13L, 14L, 15L);
          }

          @Override
          protected TypeDescriptor<Long> getRightInputType() {
            return TypeDescriptors.longs();
          }

          @Override
          public List<KV<Integer, String>> getUnorderedOutput() {
            return Arrays.asList(
                KV.of(2, "2+12"),
                KV.of(2, "2+12"),
                KV.of(4, "4+14"),
                KV.of(1, "1+11"),
                KV.of(1, "1+11"),
                KV.of(3, "3+13"),
                KV.of(3, "3+13"),
                KV.of(5, "null+15"));
          }
        });
  }

  @Test
  public void keyHashCollisionBroadcastHashJoin() {
    final String sameHashCodeKey1 = "FB";
    final String sameHashCodeKey2 = "Ea";
    execute(
        new TestCase<String, Integer, KV<String, String>>() {

          @Override
          protected PCollection<KV<String, String>> getOutput(
              PCollection<String> left, PCollection<Integer> right) {
            return LeftJoin.named("Broadcast-leftJoin")
                .of(left, MapElements.of(right).using(i -> i).output())
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
          protected TypeDescriptor<String> getLeftInputType() {
            return TypeDescriptors.strings();
          }

          @Override
          protected List<Integer> getRightInput() {
            return Arrays.asList(1, 2);
          }

          @Override
          protected TypeDescriptor<Integer> getRightInputType() {
            return TypeDescriptors.integers();
          }

          @Override
          public List<KV<String, String>> getUnorderedOutput() {
            return Arrays.asList(
                KV.of(sameHashCodeKey1, "FB+1"),
                KV.of(sameHashCodeKey2, "Ea+2"),
                KV.of("keyWithoutRightSide", "keyWithoutRightSide+null"));
          }
        });
  }
}
