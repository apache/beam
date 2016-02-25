/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.runners.inprocess;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.collect.ImmutableList;

import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * Tests for {@link InProcessBundle}.
 */
@RunWith(JUnit4.class)
public class InProcessBundleTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void unkeyedShouldCreateWithNullKey() {
    PCollection<Integer> pcollection = TestPipeline.create().apply(Create.of(1));

    InProcessBundle<Integer> inFlightBundle = InProcessBundle.unkeyed(pcollection);

    CommittedBundle<Integer> bundle = inFlightBundle.commit(Instant.now());

    assertThat(bundle.isKeyed(), is(false));
    assertThat(bundle.getKey(), nullValue());
  }

  private void keyedCreateBundle(Object key) {
    PCollection<Integer> pcollection = TestPipeline.create().apply(Create.of(1));

    InProcessBundle<Integer> inFlightBundle = InProcessBundle.keyed(pcollection, key);

    CommittedBundle<Integer> bundle = inFlightBundle.commit(Instant.now());
    assertThat(bundle.isKeyed(), is(true));
    assertThat(bundle.getKey(), equalTo(key));
  }

  @Test
  public void keyedWithNullKeyShouldCreateKeyedBundle() {
    keyedCreateBundle(null);
  }

  @Test
  public void keyedWithKeyShouldCreateKeyedBundle() {
    keyedCreateBundle(new Object());
  }

  private <T> void afterCommitGetElementsShouldHaveAddedElements(Iterable<WindowedValue<T>> elems) {
    PCollection<T> pcollection = TestPipeline.create().apply(Create.<T>of());

    InProcessBundle<T> bundle = InProcessBundle.unkeyed(pcollection);
    Collection<Matcher<? super WindowedValue<T>>> expectations = new ArrayList<>();
    for (WindowedValue<T> elem : elems) {
      bundle.add(elem);
      expectations.add(equalTo(elem));
    }
    Matcher<Iterable<? extends WindowedValue<T>>> containsMatcher =
        Matchers.<WindowedValue<T>>containsInAnyOrder(expectations);
    assertThat(bundle.commit(Instant.now()).getElements(), containsMatcher);
  }

  @Test
  public void getElementsBeforeAddShouldReturnEmptyIterable() {
    afterCommitGetElementsShouldHaveAddedElements(Collections.<WindowedValue<Integer>>emptyList());
  }

  @Test
  public void getElementsAfterAddShouldReturnAddedElements() {
    WindowedValue<Integer> firstValue = WindowedValue.valueInGlobalWindow(1);
    WindowedValue<Integer> secondValue =
        WindowedValue.timestampedValueInGlobalWindow(2, new Instant(1000L));

    afterCommitGetElementsShouldHaveAddedElements(ImmutableList.of(firstValue, secondValue));
  }

  @Test
  public void addAfterCommitShouldThrowException() {
    PCollection<Integer> pcollection = TestPipeline.create().apply(Create.<Integer>of());

    InProcessBundle<Integer> bundle = InProcessBundle.unkeyed(pcollection);
    bundle.add(WindowedValue.valueInGlobalWindow(1));
    CommittedBundle<Integer> firstCommit = bundle.commit(Instant.now());
    assertThat(firstCommit.getElements(), containsInAnyOrder(WindowedValue.valueInGlobalWindow(1)));

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("3");
    thrown.expectMessage("committed");

    bundle.add(WindowedValue.valueInGlobalWindow(3));
  }

  @Test
  public void commitAfterCommitShouldThrowException() {
    PCollection<Integer> pcollection = TestPipeline.create().apply(Create.<Integer>of());

    InProcessBundle<Integer> bundle = InProcessBundle.unkeyed(pcollection);
    bundle.add(WindowedValue.valueInGlobalWindow(1));
    CommittedBundle<Integer> firstCommit = bundle.commit(Instant.now());
    assertThat(firstCommit.getElements(), containsInAnyOrder(WindowedValue.valueInGlobalWindow(1)));

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("committed");

    bundle.commit(Instant.now());
  }
}

