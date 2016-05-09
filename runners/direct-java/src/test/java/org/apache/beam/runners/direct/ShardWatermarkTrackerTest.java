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

package org.apache.beam.runners.direct;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PDone;

import com.google.common.collect.ImmutableList;

import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link ShardWatermarkTracker}.
 */
@RunWith(JUnit4.class)
public class ShardWatermarkTrackerTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  ShardWatermarkTracker tracker;

  @Before
  public void setup() {
    tracker = ShardWatermarkTracker.create();
  }

  @Test
  public void getAfterSetReturnsMin() {
    AppliedPTransform<PBegin, PDone, PTransform<PBegin, PDone>> mytransform = getTransform();
    TransformEvaluator<?> evaluator = EmptyTransformEvaluator.create(mytransform, tracker);
    TransformEvaluator<?> otherEvaluator = EmptyTransformEvaluator.create(mytransform, tracker);
    tracker.setInitialShards(ImmutableList.of(evaluator, otherEvaluator));

    assertThat(tracker.getWatermark(), equalTo(BoundedWindow.TIMESTAMP_MIN_VALUE));

    tracker.updateWatermark(evaluator, new Instant(5));
    assertThat(tracker.getWatermark(), equalTo(BoundedWindow.TIMESTAMP_MIN_VALUE));

    tracker.updateWatermark(otherEvaluator, BoundedWindow.TIMESTAMP_MAX_VALUE);
    assertThat(tracker.getWatermark(), equalTo(new Instant(5)));

    tracker.updateWatermark(evaluator, BoundedWindow.TIMESTAMP_MAX_VALUE);
    assertThat(tracker.getWatermark(), equalTo(BoundedWindow.TIMESTAMP_MAX_VALUE));
  }

  @Test
  public void setInitialShardsWithNoneThrows() {
    thrown.expect(IllegalArgumentException.class);
    tracker.setInitialShards(ImmutableList.<TransformEvaluator<?>>of());
  }

  @Test
  public void setInitialShardsAfterSetInitialShardsThrows() {
    AppliedPTransform<PBegin, PDone, PTransform<PBegin, PDone>> mytransform = getTransform();
    TransformEvaluator<?> evaluator = EmptyTransformEvaluator.create(mytransform, tracker);
    TransformEvaluator<?> otherEvaluator = EmptyTransformEvaluator.create(mytransform, tracker);
    tracker.setInitialShards(ImmutableList.of(evaluator));

    thrown.expect(IllegalArgumentException.class);
    tracker.setInitialShards(ImmutableList.of(otherEvaluator));
  }

  @Test public void getWatermarkWithNoShardsThrows() {
    thrown.expect(IllegalStateException.class);
    tracker.getWatermark();
  }

  private AppliedPTransform<PBegin, PDone, PTransform<PBegin, PDone>> getTransform() {
    TestPipeline p = TestPipeline.create();
    PTransform<PBegin, PDone> pt = new PTransform<PBegin, PDone>() {
    };
    return AppliedPTransform.of("foo", PBegin.in(p), PDone.in(p), pt);
  }
}
