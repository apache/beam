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
package org.apache.beam.sdk.extensions.euphoria.core.client.operator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Optional;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypePropagationAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowDesc;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test operator Join. */
@RunWith(JUnit4.class)
public class JoinTest {

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testBuild() {
    final Pipeline pipeline = TestUtils.createTestPipeline();
    final PCollection<String> left =
        TestUtils.createMockDataset(pipeline, TypeDescriptors.strings());
    final PCollection<String> right =
        TestUtils.createMockDataset(pipeline, TypeDescriptors.strings());
    final PCollection<KV<Integer, String>> joined =
        Join.named("Join1")
            .of(left, right)
            .by(String::length, String::length)
            .using(
                (String l, String r, Collector<String> c) -> {
                  // no-op
                })
            .output();
    final Join join = (Join) TestUtils.getProducer(joined);
    assertTrue(join.getName().isPresent());
    assertEquals("Join1", join.getName().get());
    assertNotNull(join.getLeftKeyExtractor());
    assertNotNull(join.getRightKeyExtractor());
    assertFalse(join.getWindow().isPresent());
    assertEquals(Join.Type.INNER, join.getType());
  }

  @Test
  public void testBuild_OutputValues() {
    final Pipeline pipeline = TestUtils.createTestPipeline();
    final PCollection<String> left =
        TestUtils.createMockDataset(pipeline, TypeDescriptors.strings());
    final PCollection<String> right =
        TestUtils.createMockDataset(pipeline, TypeDescriptors.strings());

    final PCollection<String> joined =
        Join.named("JoinValues")
            .of(left, right)
            .by(String::length, String::length)
            .using(
                (String l, String r, Collector<String> c) -> {
                  // no-op
                })
            .outputValues();
    final OutputValues outputValues = (OutputValues) TestUtils.getProducer(joined);
    assertTrue(outputValues.getName().isPresent());
    assertEquals("JoinValues", outputValues.getName().get());
  }

  @Test
  public void testBuild_WithCounters() {
    final Pipeline pipeline = TestUtils.createTestPipeline();
    final PCollection<String> left =
        TestUtils.createMockDataset(pipeline, TypeDescriptors.strings());
    final PCollection<String> right =
        TestUtils.createMockDataset(pipeline, TypeDescriptors.strings());
    final PCollection<KV<Integer, String>> joined =
        Join.named("Join1")
            .of(left, right)
            .by(String::length, String::length)
            .using(
                (String l, String r, Collector<String> c) -> {
                  c.getCounter("my-counter").increment();
                  c.collect(l + r);
                })
            .output();
    final Join join = (Join) TestUtils.getProducer(joined);
    assertTrue(join.getName().isPresent());
    assertEquals("Join1", join.getName().get());
    assertNotNull(join.getLeftKeyExtractor());
    assertNotNull(join.getRightKeyExtractor());
    assertFalse(join.getWindow().isPresent());
    assertEquals(Join.Type.INNER, join.getType());
  }

  @Test
  public void testBuild_ImplicitName() {
    final Pipeline pipeline = TestUtils.createTestPipeline();
    final PCollection<String> left =
        TestUtils.createMockDataset(pipeline, TypeDescriptors.strings());
    final PCollection<String> right =
        TestUtils.createMockDataset(pipeline, TypeDescriptors.strings());
    final PCollection<KV<Integer, String>> joined =
        Join.of(left, right)
            .by(String::length, String::length)
            .using(
                (String l, String r, Collector<String> c) -> {
                  // no-op
                })
            .output();
    final Join join = (Join) TestUtils.getProducer(joined);
    assertFalse(join.getName().isPresent());
  }

  @Test
  public void testBuild_LeftJoin() {
    final Pipeline pipeline = TestUtils.createTestPipeline();
    final PCollection<String> left =
        TestUtils.createMockDataset(pipeline, TypeDescriptors.strings());
    final PCollection<String> right =
        TestUtils.createMockDataset(pipeline, TypeDescriptors.strings());
    final PCollection<KV<Integer, String>> joined =
        LeftJoin.named("Join1")
            .of(left, right)
            .by(String::length, String::length)
            .using(
                (String l, Optional<String> r, Collector<String> c) -> {
                  // no-op
                })
            .output();
    final Join join = (Join) TestUtils.getProducer(joined);
    assertEquals(Join.Type.LEFT, join.getType());
  }

  @Test
  public void testBuild_RightJoin() {
    final Pipeline pipeline = TestUtils.createTestPipeline();
    final PCollection<String> left =
        TestUtils.createMockDataset(pipeline, TypeDescriptors.strings());
    final PCollection<String> right =
        TestUtils.createMockDataset(pipeline, TypeDescriptors.strings());
    final PCollection<KV<Integer, String>> joined =
        RightJoin.named("Join1")
            .of(left, right)
            .by(String::length, String::length)
            .using(
                (Optional<String> l, String r, Collector<String> c) -> {
                  // no-op
                })
            .output();
    final Join join = (Join) TestUtils.getProducer(joined);
    assertEquals(Join.Type.RIGHT, join.getType());
  }

  @Test
  public void testBuild_FullJoin() {
    final Pipeline pipeline = TestUtils.createTestPipeline();
    final PCollection<String> left =
        TestUtils.createMockDataset(pipeline, TypeDescriptors.strings());
    final PCollection<String> right =
        TestUtils.createMockDataset(pipeline, TypeDescriptors.strings());
    final PCollection<KV<Integer, String>> joined =
        FullJoin.named("Join1")
            .of(left, right)
            .by(String::length, String::length)
            .using(
                (Optional<String> l, Optional<String> r, Collector<String> c) ->
                    c.collect(l.orElse(null) + r.orElse(null)))
            .output();
    final Join join = (Join) TestUtils.getProducer(joined);
    assertEquals(Join.Type.FULL, join.getType());
  }

  @Test
  public void testBuild_Windowing() {
    final Pipeline pipeline = TestUtils.createTestPipeline();
    final PCollection<String> left =
        TestUtils.createMockDataset(pipeline, TypeDescriptors.strings());
    final PCollection<String> right =
        TestUtils.createMockDataset(pipeline, TypeDescriptors.strings());
    final PCollection<KV<Integer, String>> joined =
        Join.named("Join1")
            .of(left, right)
            .by(String::length, String::length)
            .using((String l, String r, Collector<String> c) -> c.collect(l + r))
            .windowBy(FixedWindows.of(org.joda.time.Duration.standardHours(1)))
            .triggeredBy(AfterWatermark.pastEndOfWindow())
            .discardingFiredPanes()
            .withAllowedLateness(Duration.millis(1000))
            .output();
    final Join join = (Join) TestUtils.getProducer(joined);
    assertTrue(join.getWindow().isPresent());
    @SuppressWarnings("unchecked")
    final WindowDesc<?> windowDesc = WindowDesc.of((Window) join.getWindow().get());
    assertEquals(
        FixedWindows.of(org.joda.time.Duration.standardHours(1)), windowDesc.getWindowFn());
    assertEquals(AfterWatermark.pastEndOfWindow(), windowDesc.getTrigger());
    assertEquals(AccumulationMode.DISCARDING_FIRED_PANES, windowDesc.getAccumulationMode());
    assertEquals(Duration.millis(1000), windowDesc.getAllowedLateness());
  }

  @Test
  public void testBuild_OptionalWindowing() {
    final Pipeline pipeline = TestUtils.createTestPipeline();
    final PCollection<String> left =
        TestUtils.createMockDataset(pipeline, TypeDescriptors.strings());
    final PCollection<String> right =
        TestUtils.createMockDataset(pipeline, TypeDescriptors.strings());
    final PCollection<KV<Integer, String>> joined =
        Join.named("Join1")
            .of(left, right)
            .by(String::length, String::length)
            .using((String l, String r, Collector<String> c) -> c.collect(l + r))
            .applyIf(
                true,
                b ->
                    b.windowBy(FixedWindows.of(org.joda.time.Duration.standardHours(1)))
                        .triggeredBy(AfterWatermark.pastEndOfWindow())
                        .accumulationMode(AccumulationMode.DISCARDING_FIRED_PANES))
            .output();
    final Join join = (Join) TestUtils.getProducer(joined);
    assertTrue(join.getWindow().isPresent());
    final Window<?> window = (Window) join.getWindow().get();
    assertEquals(FixedWindows.of(org.joda.time.Duration.standardHours(1)), window.getWindowFn());
    assertEquals(AfterWatermark.pastEndOfWindow(), WindowDesc.of(window).getTrigger());
    assertEquals(
        AccumulationMode.DISCARDING_FIRED_PANES, WindowDesc.of(window).getAccumulationMode());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBuildTypePropagation() {
    final Pipeline pipeline = TestUtils.createTestPipeline();
    final PCollection<String> left =
        TestUtils.createMockDataset(pipeline, TypeDescriptors.strings());
    final PCollection<String> right =
        TestUtils.createMockDataset(pipeline, TypeDescriptors.strings());
    final TypeDescriptor<Integer> keyType = TypeDescriptors.integers();
    final TypeDescriptor<String> outputType = TypeDescriptors.strings();
    final PCollection<KV<Integer, String>> joined =
        Join.named("Join1")
            .of(left, right)
            .by(String::length, String::length, keyType)
            .using(
                (String l, String r, Collector<String> c) -> {
                  // no-op
                },
                outputType)
            .output();
    final Join join = (Join) TestUtils.getProducer(joined);
    TypePropagationAssert.assertOperatorTypeAwareness(join, keyType, outputType);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBuild_LeftJoinTypePropagation() {
    final Pipeline pipeline = TestUtils.createTestPipeline();
    final PCollection<String> left =
        TestUtils.createMockDataset(pipeline, TypeDescriptors.strings());
    final PCollection<String> right =
        TestUtils.createMockDataset(pipeline, TypeDescriptors.strings());
    TypeDescriptor<Integer> keyType = TypeDescriptors.integers();
    TypeDescriptor<String> outputType = TypeDescriptors.strings();
    final PCollection<KV<Integer, String>> joined =
        LeftJoin.named("Join1")
            .of(left, right)
            .by(String::length, String::length, keyType)
            .using(
                (String l, Optional<String> r, Collector<String> c) -> {
                  // no-op
                },
                outputType)
            .output();
    final Join join = (Join) TestUtils.getProducer(joined);
    TypePropagationAssert.assertOperatorTypeAwareness(join, keyType, outputType);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBuild_RightJoinTypePropagation() {
    final Pipeline pipeline = TestUtils.createTestPipeline();
    final PCollection<String> left =
        TestUtils.createMockDataset(pipeline, TypeDescriptors.strings());
    final PCollection<String> right =
        TestUtils.createMockDataset(pipeline, TypeDescriptors.strings());
    final TypeDescriptor<Integer> keyType = TypeDescriptors.integers();
    final TypeDescriptor<String> outputType = TypeDescriptors.strings();
    final PCollection<KV<Integer, String>> joined =
        RightJoin.named("Join1")
            .of(left, right)
            .by(String::length, String::length, keyType)
            .using(
                (Optional<String> l, String r, Collector<String> c) -> {
                  // no-op
                },
                outputType)
            .output();
    final Join join = (Join) TestUtils.getProducer(joined);
    TypePropagationAssert.assertOperatorTypeAwareness(join, keyType, outputType);
  }
}
