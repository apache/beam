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
package org.apache.beam.sdk.io.gcp.firestore;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.ItemSpec;
import org.apache.beam.sdk.util.SerializableUtils;
import org.joda.time.Duration;
import org.junit.Test;
import org.junit.runners.model.MultipleFailureException;
import org.mockito.ArgumentCaptor;

@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public final class RpcQosOptionsTest {

  @Test
  public void ensureSerializable() {
    SerializableUtils.ensureSerializable(RpcQosOptions.defaultOptions());
  }

  @Test
  public void builderBuildBuilder() {
    RpcQosOptions rpcQosOptions = RpcQosOptions.defaultOptions();
    int newMaxAttempts = rpcQosOptions.getMaxAttempts() - 1;
    RpcQosOptions.Builder builder = rpcQosOptions.toBuilder().withMaxAttempts(newMaxAttempts);
    RpcQosOptions build = builder.build();

    assertNotEquals(rpcQosOptions, build);
    assertEquals(newMaxAttempts, build.getMaxAttempts());
  }

  @Test
  public void populateDisplayData() {
    //noinspection unchecked
    ArgumentCaptor<ItemSpec<?>> captor = ArgumentCaptor.forClass(DisplayData.ItemSpec.class);

    DisplayData.Builder builder = mock(DisplayData.Builder.class);
    when(builder.add(captor.capture())).thenReturn(builder);
    RpcQosOptions rpcQosOptions = RpcQosOptions.defaultOptions();
    rpcQosOptions.populateDisplayData(builder);

    List<String> actualKeys =
        captor.getAllValues().stream().map(ItemSpec::getKey).sorted().collect(Collectors.toList());

    List<String> expectedKeys =
        newArrayList(
            "batchInitialCount",
            "batchMaxBytes",
            "batchMaxCount",
            "batchTargetLatency",
            "hintMaxNumWorkers",
            "initialBackoff",
            "maxAttempts",
            "overloadRatio",
            "samplePeriod",
            "samplePeriodBucketSize",
            "shouldReportDiagnosticMetrics",
            "throttleDuration");

    assertEquals(expectedKeys, actualKeys);
  }

  @Test
  public void defaultOptionsBuildSuccessfully() {
    assertNotNull(RpcQosOptions.defaultOptions());
  }

  @Test
  public void argumentValidation_maxAttempts() throws Exception {
    BiFunction<RpcQosOptions.Builder, Integer, RpcQosOptions.Builder> f =
        RpcQosOptions.Builder::withMaxAttempts;
    testNullability(f);
    testIntRange(f, 1, 5);
  }

  @Test
  public void argumentValidation_withInitialBackoff() throws Exception {
    BiFunction<RpcQosOptions.Builder, Duration, RpcQosOptions.Builder> f =
        RpcQosOptions.Builder::withInitialBackoff;
    testNullability(f);
    testDurationRange(f, Duration.standardSeconds(5), Duration.standardMinutes(2));
  }

  @Test
  public void argumentValidation_withSamplePeriod() throws Exception {
    BiFunction<RpcQosOptions.Builder, Duration, RpcQosOptions.Builder> f =
        RpcQosOptions.Builder::withSamplePeriod;
    testNullability(f);
    testDurationRange(f, Duration.standardMinutes(2), Duration.standardMinutes(20));
  }

  @Test
  public void argumentValidation_withSampleUpdateFrequency() throws Exception {
    BiFunction<RpcQosOptions.Builder, Duration, RpcQosOptions.Builder> f =
        RpcQosOptions.Builder::withSamplePeriodBucketSize;
    testNullability(f);
    testDurationRange(f, Duration.standardSeconds(10), Duration.standardMinutes(20));
  }

  @Test
  public void argumentValidation_withSampleUpdateFrequency_lteqSamplePeriod() {
    RpcQosOptions.newBuilder()
        .withSamplePeriod(Duration.millis(5))
        .withSamplePeriodBucketSize(Duration.millis(5))
        .validateRelatedFields();
    try {
      RpcQosOptions.newBuilder()
          .withSamplePeriod(Duration.millis(5))
          .withSamplePeriodBucketSize(Duration.millis(6))
          .validateRelatedFields();
      fail("expected validation failure for samplePeriodBucketSize > samplePeriod");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("samplePeriodBucketSize <= samplePeriod"));
    }
  }

  @Test
  public void argumentValidation_withOverloadRatio() throws Exception {
    BiFunction<RpcQosOptions.Builder, Double, RpcQosOptions.Builder> f =
        RpcQosOptions.Builder::withOverloadRatio;
    testNullability(f);
    testRange(0.0001, Double::sum, (a, b) -> a - b, f, 1.0, 1.5);
  }

  @Test
  public void argumentValidation_withThrottleDuration() throws Exception {
    BiFunction<RpcQosOptions.Builder, Duration, RpcQosOptions.Builder> f =
        RpcQosOptions.Builder::withThrottleDuration;
    testNullability(f);
    testDurationRange(f, Duration.standardSeconds(5), Duration.standardMinutes(1));
  }

  @Test
  public void argumentValidation_withBatchInitialCount() throws Exception {
    BiFunction<RpcQosOptions.Builder, Integer, RpcQosOptions.Builder> f =
        RpcQosOptions.Builder::withBatchInitialCount;
    testNullability(f);
    testIntRange(f, 1, 500);
  }

  @Test
  public void argumentValidation_withBatchMaxCount() throws Exception {
    BiFunction<RpcQosOptions.Builder, Integer, RpcQosOptions.Builder> f =
        RpcQosOptions.Builder::withBatchMaxCount;
    testNullability(f);
    testIntRange(f, 1, 500);
  }

  @Test
  public void argumentValidation_withBatchMaxBytes() throws Exception {
    BiFunction<RpcQosOptions.Builder, Long, RpcQosOptions.Builder> f =
        RpcQosOptions.Builder::withBatchMaxBytes;
    testNullability(f);
    testRange(1L, Math::addExact, Math::subtractExact, f, 1L, (long) (9.5 * 1024 * 1024));
  }

  @Test
  public void argumentValidation_withBatchTargetLatency() throws Exception {
    BiFunction<RpcQosOptions.Builder, Duration, RpcQosOptions.Builder> f =
        RpcQosOptions.Builder::withBatchTargetLatency;
    testNullability(f);
    testDurationRange(f, Duration.standardSeconds(5), Duration.standardMinutes(2));
  }

  private static <T> void testNullability(
      BiFunction<RpcQosOptions.Builder, T, RpcQosOptions.Builder> f) {
    try {
      f.apply(RpcQosOptions.newBuilder(), null).build();
      fail("expected NullPointerException");
    } catch (NullPointerException e) {
      // pass
    }
  }

  private static void testIntRange(
      BiFunction<RpcQosOptions.Builder, Integer, RpcQosOptions.Builder> f, int min, int max)
      throws Exception {
    testRange(1, Math::addExact, Math::subtractExact, f, min, max);
  }

  private static void testDurationRange(
      BiFunction<RpcQosOptions.Builder, Duration, RpcQosOptions.Builder> f,
      Duration min,
      Duration max)
      throws Exception {
    testRange(Duration.millis(1), Duration::plus, Duration::minus, f, min, max);
  }

  private static <T> void testRange(
      T epsilon,
      BiFunction<T, T, T> plus,
      BiFunction<T, T, T> minus,
      BiFunction<RpcQosOptions.Builder, T, RpcQosOptions.Builder> f,
      T min,
      T max)
      throws Exception {
    List<Throwable> errors = new ArrayList<>();
    errors.addAll(testMinBoundary(epsilon, plus, minus, f, min));
    errors.addAll(testMaxBoundary(epsilon, plus, minus, f, max));
    MultipleFailureException.assertEmpty(errors);
  }

  private static <T> List<Throwable> testMaxBoundary(
      T epsilon,
      BiFunction<T, T, T> plus,
      BiFunction<T, T, T> minus,
      BiFunction<RpcQosOptions.Builder, T, RpcQosOptions.Builder> f,
      T max) {
    List<Throwable> errors = new ArrayList<>();
    try {
      f.apply(RpcQosOptions.newBuilder(), minus.apply(max, epsilon)).validateIndividualFields();
    } catch (Throwable t) {
      errors.add(newError(t, "max - epsilon"));
    }
    try {
      f.apply(RpcQosOptions.newBuilder(), max).validateIndividualFields();
    } catch (Throwable t) {
      errors.add(newError(t, "max"));
    }
    try {
      try {
        f.apply(RpcQosOptions.newBuilder(), plus.apply(max, epsilon)).validateIndividualFields();
        fail("expected IllegalArgumentException");
      } catch (IllegalArgumentException e) {
        // pass
      }
    } catch (Throwable t) {
      errors.add(newError(t, "max + epsilon"));
    }
    return errors;
  }

  private static <T> List<Throwable> testMinBoundary(
      T epsilon,
      BiFunction<T, T, T> plus,
      BiFunction<T, T, T> minus,
      BiFunction<RpcQosOptions.Builder, T, RpcQosOptions.Builder> f,
      T min) {
    List<Throwable> errors = new ArrayList<>();
    try {
      try {
        f.apply(RpcQosOptions.newBuilder(), minus.apply(min, epsilon)).validateIndividualFields();
        fail("expected IllegalArgumentException");
      } catch (IllegalArgumentException e) {
        // pass
      }
    } catch (Throwable t) {
      errors.add(newError(t, "min - epsilon"));
    }
    try {
      f.apply(RpcQosOptions.newBuilder(), min).validateIndividualFields();
    } catch (Throwable t) {
      errors.add(newError(t, "min"));
    }
    try {
      f.apply(RpcQosOptions.newBuilder(), plus.apply(min, epsilon)).validateIndividualFields();
    } catch (Throwable t) {
      errors.add(newError(t, "min + epsilon"));
    }
    return errors;
  }

  private static AssertionError newError(Throwable t, String conditionDescription) {
    return new AssertionError(
        String.format("error while testing boundary condition (%s)", conditionDescription), t);
  }
}
