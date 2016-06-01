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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import org.apache.beam.runners.direct.InProcessPipelineRunner.CommittedBundle;
import org.apache.beam.runners.direct.InProcessPipelineRunner.UncommittedBundle;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import com.google.common.collect.ImmutableList;

import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * Tests for {@link InProcessBundleFactory}.
 */
@RunWith(JUnit4.class)
public class InProcessBundleFactoryTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  private InProcessBundleFactory bundleFactory = InProcessBundleFactory.create();

  private PCollection<Integer> created;
  private PCollection<KV<String, Integer>> downstream;

  @Before
  public void setup() {
    TestPipeline p = TestPipeline.create();
    created = p.apply(Create.of(1, 2, 3));
    downstream = created.apply(WithKeys.<String, Integer>of("foo"));
  }

  @Test
  public void createRootBundleShouldCreateWithEmptyKey() {
    PCollection<Integer> pcollection = TestPipeline.create().apply(Create.of(1));

    UncommittedBundle<Integer> inFlightBundle = bundleFactory.createRootBundle(pcollection);

    CommittedBundle<Integer> bundle = inFlightBundle.commit(Instant.now());

    assertThat(bundle.getKey(),
        Matchers.<StructuralKey<?>>equalTo(StructuralKey.of(null, VoidCoder.of())));
  }

  private <T> void createKeyedBundle(Coder<T> coder, T key) throws Exception {
    PCollection<Integer> pcollection = TestPipeline.create().apply(Create.of(1));
    StructuralKey skey = StructuralKey.of(key, coder);

    UncommittedBundle<Integer> inFlightBundle =
        bundleFactory.createKeyedBundle(null, skey, pcollection);

    CommittedBundle<Integer> bundle = inFlightBundle.commit(Instant.now());
    assertThat(bundle.getKey(), equalTo(skey));
  }

  @Test
  public void keyedWithNullKeyShouldCreateKeyedBundle() throws Exception {
    createKeyedBundle(VoidCoder.of(), null);
  }

  @Test
  public void keyedWithKeyShouldCreateKeyedBundle() throws Exception {
    createKeyedBundle(StringUtf8Coder.of(), "foo");
    createKeyedBundle(VarIntCoder.of(), 1234);
    createKeyedBundle(ByteArrayCoder.of(), new byte[] {0, 2, 4, 99});
  }

  private <T> CommittedBundle<T>
  afterCommitGetElementsShouldHaveAddedElements(Iterable<WindowedValue<T>> elems) {
    PCollection<T> pcollection = TestPipeline.create().apply(Create.<T>of());

    UncommittedBundle<T> bundle = bundleFactory.createRootBundle(pcollection);
    Collection<Matcher<? super WindowedValue<T>>> expectations = new ArrayList<>();
    for (WindowedValue<T> elem : elems) {
      bundle.add(elem);
      expectations.add(equalTo(elem));
    }
    Matcher<Iterable<? extends WindowedValue<T>>> containsMatcher =
        Matchers.<WindowedValue<T>>containsInAnyOrder(expectations);
    CommittedBundle<T> committed = bundle.commit(Instant.now());
    assertThat(committed.getElements(), containsMatcher);

    return committed;
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

  @SuppressWarnings("unchecked")
  @Test
  public void withElementsShouldReturnIndependentBundle() {
    WindowedValue<Integer> firstValue = WindowedValue.valueInGlobalWindow(1);
    WindowedValue<Integer> secondValue =
        WindowedValue.timestampedValueInGlobalWindow(2, new Instant(1000L));

    CommittedBundle<Integer> committed =
        afterCommitGetElementsShouldHaveAddedElements(ImmutableList.of(firstValue, secondValue));

    WindowedValue<Integer> firstReplacement =
        WindowedValue.of(
            9,
            new Instant(2048L),
            new IntervalWindow(new Instant(2044L), Instant.now()),
            PaneInfo.NO_FIRING);
    WindowedValue<Integer> secondReplacement =
        WindowedValue.timestampedValueInGlobalWindow(-1, Instant.now());
    CommittedBundle<Integer> withed =
        committed.withElements(ImmutableList.of(firstReplacement, secondReplacement));

    assertThat(withed.getElements(), containsInAnyOrder(firstReplacement, secondReplacement));
    assertThat(committed.getElements(), containsInAnyOrder(firstValue, secondValue));
    assertThat(withed.getKey(), Matchers.<StructuralKey<?>>equalTo(committed.getKey()));
    assertThat(withed.getPCollection(), equalTo(committed.getPCollection()));
    assertThat(
        withed.getSynchronizedProcessingOutputWatermark(),
        equalTo(committed.getSynchronizedProcessingOutputWatermark()));
  }

  @Test
  public void addAfterCommitShouldThrowException() {
    PCollection<Integer> pcollection = TestPipeline.create().apply(Create.<Integer>of());

    UncommittedBundle<Integer> bundle = bundleFactory.createRootBundle(pcollection);
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

    UncommittedBundle<Integer> bundle = bundleFactory.createRootBundle(pcollection);
    bundle.add(WindowedValue.valueInGlobalWindow(1));
    CommittedBundle<Integer> firstCommit = bundle.commit(Instant.now());
    assertThat(firstCommit.getElements(), containsInAnyOrder(WindowedValue.valueInGlobalWindow(1)));

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("committed");

    bundle.commit(Instant.now());
  }

  @Test
  public void createBundleUnkeyedResultUnkeyed() {
    CommittedBundle<KV<String, Integer>> newBundle =
        bundleFactory
            .createBundle(bundleFactory.createRootBundle(created).commit(Instant.now()), downstream)
            .commit(Instant.now());
  }

  @Test
  public void createBundleKeyedResultPropagatesKey() {
    CommittedBundle<KV<String, Integer>> newBundle =
        bundleFactory.createBundle(
            bundleFactory.createKeyedBundle(
                null,
                StructuralKey.of("foo", StringUtf8Coder.of()),
                created).commit(Instant.now()),
            downstream).commit(Instant.now());
    assertThat(newBundle.getKey().getKey(), Matchers.<Object>equalTo("foo"));
  }

  @Test
  public void createKeyedBundleKeyed() {
    CommittedBundle<KV<String, Integer>> keyedBundle = bundleFactory.createKeyedBundle(
        bundleFactory.createRootBundle(created).commit(Instant.now()),
        StructuralKey.of("foo", StringUtf8Coder.of()),
        downstream).commit(Instant.now());
    assertThat(keyedBundle.getKey().getKey(), Matchers.<Object>equalTo("foo"));
  }
}
