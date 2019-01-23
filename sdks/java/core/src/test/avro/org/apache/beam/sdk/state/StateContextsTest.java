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
package org.apache.beam.sdk.state;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link StateContexts}. */
@RunWith(JUnit4.class)
public class StateContextsTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  private PCollectionView<Integer> view;

  @Before
  public void setup() {
    Pipeline p = Pipeline.create();
    view = p.apply(Create.of(1)).apply(Sum.integersGlobally().asSingletonView());
  }

  @Test
  public void nullContextThrowsOnWindow() {
    StateContext<BoundedWindow> context = StateContexts.nullContext();
    thrown.expect(IllegalArgumentException.class);
    context.window();
  }

  @Test
  public void nullContextThrowsOnSideInput() {
    StateContext<BoundedWindow> context = StateContexts.nullContext();
    thrown.expect(IllegalArgumentException.class);
    context.sideInput(view);
  }

  @Test
  public void nullContextThrowsOnOptions() {
    StateContext<BoundedWindow> context = StateContexts.nullContext();
    thrown.expect(IllegalArgumentException.class);
    context.getPipelineOptions();
  }

  @Test
  public void windowOnlyContextThrowsOnOptions() {
    BoundedWindow window = new IntervalWindow(new Instant(-137), Duration.millis(21L));
    StateContext<BoundedWindow> context = StateContexts.windowOnlyContext(window);
    thrown.expect(IllegalArgumentException.class);
    context.getPipelineOptions();
  }

  @Test
  public void windowOnlyContextThrowsOnSideInput() {
    BoundedWindow window = new IntervalWindow(new Instant(-137), Duration.millis(21L));
    StateContext<BoundedWindow> context = StateContexts.windowOnlyContext(window);
    thrown.expect(IllegalArgumentException.class);
    context.sideInput(view);
  }

  @Test
  public void windowOnlyContextWindowReturnsWindow() {
    BoundedWindow window = new IntervalWindow(new Instant(-137), Duration.millis(21L));
    StateContext<BoundedWindow> context = StateContexts.windowOnlyContext(window);
    assertThat(context.window(), equalTo(window));
  }
}
