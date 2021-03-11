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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import org.apache.beam.runners.local.StructuralKey;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.IllegalMutationException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ImmutabilityCheckingBundleFactory}. */
@RunWith(JUnit4.class)
public class ImmutabilityCheckingBundleFactoryTest {

  @Rule public final TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);
  @Rule public ExpectedException thrown = ExpectedException.none();
  private ImmutabilityCheckingBundleFactory factory;
  private PCollection<byte[]> created;
  private PCollection<byte[]> transformed;

  @Before
  public void setup() {
    created = p.apply(Create.empty(ByteArrayCoder.of()));
    transformed = created.apply(ParDo.of(new IdentityDoFn<>()));
    DirectGraphVisitor visitor = new DirectGraphVisitor();
    p.traverseTopologically(visitor);
    factory =
        ImmutabilityCheckingBundleFactory.create(
            ImmutableListBundleFactory.create(), visitor.getGraph());
  }

  @Test
  public void rootBundleSucceeds() {
    UncommittedBundle<byte[]> root = factory.createRootBundle();
    byte[] array = new byte[] {0, 1, 2};
    root.add(WindowedValue.valueInGlobalWindow(array));
    CommittedBundle<byte[]> committed = root.commit(Instant.now());

    assertThat(
        committed.getElements(), containsInAnyOrder(WindowedValue.valueInGlobalWindow(array)));
  }

  @Test
  public void noMutationKeyedBundleSucceeds() {
    UncommittedBundle<byte[]> keyed =
        factory.createKeyedBundle(StructuralKey.of("mykey", StringUtf8Coder.of()), transformed);

    WindowedValue<byte[]> windowedArray =
        WindowedValue.of(
            new byte[] {4, 8, 12},
            new Instant(891L),
            new IntervalWindow(new Instant(0), new Instant(1000)),
            PaneInfo.ON_TIME_AND_ONLY_FIRING);
    keyed.add(windowedArray);

    CommittedBundle<byte[]> committed = keyed.commit(Instant.now());
    assertThat(committed.getElements(), containsInAnyOrder(windowedArray));
  }

  @Test
  public void noMutationCreateBundleSucceeds() {
    UncommittedBundle<byte[]> intermediate = factory.createBundle(transformed);

    WindowedValue<byte[]> windowedArray =
        WindowedValue.of(
            new byte[] {4, 8, 12},
            new Instant(891L),
            new IntervalWindow(new Instant(0), new Instant(1000)),
            PaneInfo.ON_TIME_AND_ONLY_FIRING);
    intermediate.add(windowedArray);

    CommittedBundle<byte[]> committed = intermediate.commit(Instant.now());
    assertThat(committed.getElements(), containsInAnyOrder(windowedArray));
  }

  @Test
  public void mutationBeforeAddKeyedBundleSucceeds() {
    UncommittedBundle<byte[]> keyed =
        factory.createKeyedBundle(StructuralKey.of("mykey", StringUtf8Coder.of()), transformed);

    byte[] array = new byte[] {4, 8, 12};
    array[0] = Byte.MAX_VALUE;
    WindowedValue<byte[]> windowedArray =
        WindowedValue.of(
            array,
            new Instant(891L),
            new IntervalWindow(new Instant(0), new Instant(1000)),
            PaneInfo.ON_TIME_AND_ONLY_FIRING);
    keyed.add(windowedArray);

    CommittedBundle<byte[]> committed = keyed.commit(Instant.now());
    assertThat(committed.getElements(), containsInAnyOrder(windowedArray));
  }

  @Test
  public void mutationBeforeAddCreateBundleSucceeds() {
    UncommittedBundle<byte[]> intermediate = factory.createBundle(transformed);

    byte[] array = new byte[] {4, 8, 12};
    WindowedValue<byte[]> windowedArray =
        WindowedValue.of(
            array,
            new Instant(891L),
            new IntervalWindow(new Instant(0), new Instant(1000)),
            PaneInfo.ON_TIME_AND_ONLY_FIRING);
    array[2] = -3;
    intermediate.add(windowedArray);

    CommittedBundle<byte[]> committed = intermediate.commit(Instant.now());
    assertThat(committed.getElements(), containsInAnyOrder(windowedArray));
  }

  @Test
  public void mutationAfterAddKeyedBundleThrows() {
    UncommittedBundle<byte[]> keyed =
        factory.createKeyedBundle(StructuralKey.of("mykey", StringUtf8Coder.of()), transformed);

    byte[] array = new byte[] {4, 8, 12};
    WindowedValue<byte[]> windowedArray =
        WindowedValue.of(
            array,
            new Instant(891L),
            new IntervalWindow(new Instant(0), new Instant(1000)),
            PaneInfo.ON_TIME_AND_ONLY_FIRING);
    keyed.add(windowedArray);

    array[0] = Byte.MAX_VALUE;
    thrown.expect(IllegalMutationException.class);
    thrown.expectMessage("Values must not be mutated in any way after being output");
    keyed.commit(Instant.now());
  }

  @Test
  public void mutationAfterAddCreateBundleThrows() {
    UncommittedBundle<byte[]> intermediate = factory.createBundle(transformed);

    byte[] array = new byte[] {4, 8, 12};
    WindowedValue<byte[]> windowedArray =
        WindowedValue.of(
            array,
            new Instant(891L),
            new IntervalWindow(new Instant(0), new Instant(1000)),
            PaneInfo.ON_TIME_AND_ONLY_FIRING);
    intermediate.add(windowedArray);

    array[2] = -3;
    thrown.expect(IllegalMutationException.class);
    thrown.expectMessage("Values must not be mutated in any way after being output");
    intermediate.commit(Instant.now());
  }

  private static class IdentityDoFn<T> extends DoFn<T, T> {
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      c.output(c.element());
    }
  }
}
