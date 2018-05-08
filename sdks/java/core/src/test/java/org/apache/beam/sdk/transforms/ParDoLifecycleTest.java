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

package org.apache.beam.sdk.transforms;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesParDoLifecycle;
import org.apache.beam.sdk.testing.UsesStatefulParDo;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests that {@link ParDo} exercises {@link DoFn} methods in the appropriate sequence.
 */
@RunWith(JUnit4.class)
public class ParDoLifecycleTest implements Serializable {

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  private static class CallSequenceEnforcingDoFn<T> extends DoFn<T, T> {
    private boolean setupCalled = false;
    private int startBundleCalls = 0;
    private int finishBundleCalls = 0;
    private boolean teardownCalled = false;

    @Setup
    public void setup() {
      assertThat("setup should not be called twice", setupCalled, is(false));
      assertThat("setup should be called before startBundle", startBundleCalls, equalTo(0));
      assertThat("setup should be called before finishBundle", finishBundleCalls, equalTo(0));
      assertThat("setup should be called before teardown", teardownCalled, is(false));
      setupCalled = true;
    }

    @StartBundle
    public void startBundle() {
      assertThat("setup should have been called", setupCalled, is(true));
      assertThat(
          "Even number of startBundle and finishBundle calls in startBundle",
          startBundleCalls,
          equalTo(finishBundleCalls));
      assertThat("teardown should not have been called", teardownCalled, is(false));
      startBundleCalls++;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      assertThat("startBundle should have been called", startBundleCalls, greaterThan(0));
      assertThat(
          "there should be one startBundle call with no call to finishBundle",
          startBundleCalls,
          equalTo(finishBundleCalls + 1));
      assertThat("teardown should not have been called", teardownCalled, is(false));
    }

    @FinishBundle
    public void finishBundle() {
      assertThat("startBundle should have been called", startBundleCalls, greaterThan(0));
      assertThat(
          "there should be one bundle that has been started but not finished",
          startBundleCalls,
          equalTo(finishBundleCalls + 1));
      assertThat("teardown should not have been called", teardownCalled, is(false));
      finishBundleCalls++;
    }

    @Teardown
    public void teardown() {
      assertThat(setupCalled, is(true));
      assertThat(startBundleCalls, anyOf(equalTo(finishBundleCalls)));
      assertThat(teardownCalled, is(false));
      teardownCalled = true;
    }
  }

  @Test
  @Category({ValidatesRunner.class, UsesParDoLifecycle.class})
  public void testFnCallSequence() {
    PCollectionList.of(p.apply("Impolite", Create.of(1, 2, 4)))
        .and(p.apply("Polite", Create.of(3, 5, 6, 7)))
        .apply(Flatten.pCollections())
        .apply(ParDo.of(new CallSequenceEnforcingFn<>()));

    p.run();
  }

  @Test
  @Category({ValidatesRunner.class, UsesParDoLifecycle.class})
  public void testFnCallSequenceMulti() {
    PCollectionList.of(p.apply("Impolite", Create.of(1, 2, 4)))
        .and(p.apply("Polite", Create.of(3, 5, 6, 7)))
        .apply(Flatten.pCollections())
        .apply(
            ParDo.of(new CallSequenceEnforcingFn<Integer>())
                .withOutputTags(new TupleTag<Integer>() {}, TupleTagList.empty()));

    p.run();
  }

  @Test
  @Category({ValidatesRunner.class, UsesStatefulParDo.class, UsesParDoLifecycle.class})
  public void testFnCallSequenceStateful() {
    PCollectionList.of(p.apply("Impolite", Create.of(KV.of("a", 1), KV.of("b", 2), KV.of("a", 4))))
        .and(
            p.apply(
                "Polite", Create.of(KV.of("b", 3), KV.of("a", 5), KV.of("c", 6), KV.of("c", 7))))
        .apply(Flatten.pCollections())
        .apply(
            ParDo.of(new CallSequenceEnforcingStatefulFn<String, Integer>())
                .withOutputTags(new TupleTag<KV<String, Integer>>() {}, TupleTagList.empty()));

    p.run();
  }

  private static class CallSequenceEnforcingFn<T> extends DoFn<T, T> {
    private boolean setupCalled = false;
    private int startBundleCalls = 0;
    private int finishBundleCalls = 0;
    private boolean teardownCalled = false;

    @Setup
    public void before() {
      assertThat("setup should not be called twice", setupCalled, is(false));
      assertThat("setup should be called before startBundle", startBundleCalls, equalTo(0));
      assertThat("setup should be called before finishBundle", finishBundleCalls, equalTo(0));
      assertThat("setup should be called before teardown", teardownCalled, is(false));
      setupCalled = true;
    }

    @StartBundle
    public void begin() {
      assertThat("setup should have been called", setupCalled, is(true));
      assertThat("Even number of startBundle and finishBundle calls in startBundle",
          startBundleCalls,
          equalTo(finishBundleCalls));
      assertThat("teardown should not have been called", teardownCalled, is(false));
      startBundleCalls++;
    }

    @ProcessElement
    public void process(ProcessContext c) throws Exception {
      assertThat("startBundle should have been called", startBundleCalls, greaterThan(0));
      assertThat("there should be one startBundle call with no call to finishBundle",
          startBundleCalls,
          equalTo(finishBundleCalls + 1));
      assertThat("teardown should not have been called", teardownCalled, is(false));
    }

    @FinishBundle
    public void end() {
      assertThat("startBundle should have been called", startBundleCalls, greaterThan(0));
      assertThat("there should be one bundle that has been started but not finished",
          startBundleCalls,
          equalTo(finishBundleCalls + 1));
      assertThat("teardown should not have been called", teardownCalled, is(false));
      finishBundleCalls++;
    }

    @Teardown
    public void after() {
      assertThat(setupCalled, is(true));
      assertThat(startBundleCalls, anyOf(equalTo(finishBundleCalls)));
      assertThat(teardownCalled, is(false));
      teardownCalled = true;
    }
  }

  private static class CallSequenceEnforcingStatefulFn<K, V>
      extends CallSequenceEnforcingDoFn<KV<K, V>> {
    private static final String STATE_ID = "foo";

    @StateId(STATE_ID)
    private final StateSpec<ValueState<String>> valueSpec = StateSpecs.value();
  }

  @Test
  @Category({ValidatesRunner.class, UsesParDoLifecycle.class})
  public void testTeardownCalledAfterExceptionInStartBundle() {
    ExceptionThrowingOldFn fn = new ExceptionThrowingOldFn(MethodForException.START_BUNDLE);
    p
        .apply(Create.of(1, 2, 3))
        .apply(ParDo.of(fn));
    try {
      p.run();
      fail("Pipeline should have failed with an exception");
    } catch (Exception e) {
      assertThat(
          "Function should have been torn down after exception",
          ExceptionThrowingOldFn.teardownCalled.get(),
          is(true));
    }
  }

  @Test
  @Category({ValidatesRunner.class, UsesParDoLifecycle.class})
  public void testTeardownCalledAfterExceptionInProcessElement() {
    ExceptionThrowingOldFn fn = new ExceptionThrowingOldFn(MethodForException.PROCESS_ELEMENT);
    p
        .apply(Create.of(1, 2, 3))
        .apply(ParDo.of(fn));
    try {
      p.run();
      fail("Pipeline should have failed with an exception");
    } catch (Exception e) {
      assertThat(
          "Function should have been torn down after exception",
          ExceptionThrowingOldFn.teardownCalled.get(),
          is(true));
    }
  }

  @Test
  @Category({ValidatesRunner.class, UsesParDoLifecycle.class})
  public void testTeardownCalledAfterExceptionInFinishBundle() {
    ExceptionThrowingOldFn fn = new ExceptionThrowingOldFn(MethodForException.FINISH_BUNDLE);
    p
        .apply(Create.of(1, 2, 3))
        .apply(ParDo.of(fn));
    try {
      p.run();
      fail("Pipeline should have failed with an exception");
    } catch (Exception e) {
      assertThat(
          "Function should have been torn down after exception",
          ExceptionThrowingOldFn.teardownCalled.get(),
          is(true));
    }
  }

  @Test
  @Category({ValidatesRunner.class, UsesParDoLifecycle.class})
  public void testWithContextTeardownCalledAfterExceptionInSetup() {
    ExceptionThrowingOldFn fn = new ExceptionThrowingOldFn(MethodForException.SETUP);
    p.apply(Create.of(1, 2, 3)).apply(ParDo.of(fn));
    try {
      p.run();
      fail("Pipeline should have failed with an exception");
    } catch (Exception e) {
      assertThat("Function should have been torn down after exception",
          ExceptionThrowingOldFn.teardownCalled.get(),
          is(true));
    }
  }

  @Test
  @Category({ValidatesRunner.class, UsesParDoLifecycle.class})
  public void testWithContextTeardownCalledAfterExceptionInStartBundle() {
    ExceptionThrowingOldFn fn = new ExceptionThrowingOldFn(MethodForException.START_BUNDLE);
    p.apply(Create.of(1, 2, 3)).apply(ParDo.of(fn));
    try {
      p.run();
      fail("Pipeline should have failed with an exception");
    } catch (Exception e) {
      assertThat("Function should have been torn down after exception",
          ExceptionThrowingOldFn.teardownCalled.get(),
          is(true));
    }
  }

  @Test
  @Category({ValidatesRunner.class, UsesParDoLifecycle.class})
  public void testWithContextTeardownCalledAfterExceptionInProcessElement() {
    ExceptionThrowingOldFn fn = new ExceptionThrowingOldFn(MethodForException.PROCESS_ELEMENT);
    p.apply(Create.of(1, 2, 3)).apply(ParDo.of(fn));
    try {
      p.run();
      fail("Pipeline should have failed with an exception");
    } catch (Exception e) {
      assertThat("Function should have been torn down after exception",
          ExceptionThrowingOldFn.teardownCalled.get(),
          is(true));
    }
  }

  @Test
  @Category({ValidatesRunner.class, UsesParDoLifecycle.class})
  public void testWithContextTeardownCalledAfterExceptionInFinishBundle() {
    ExceptionThrowingOldFn fn = new ExceptionThrowingOldFn(MethodForException.FINISH_BUNDLE);
    p.apply(Create.of(1, 2, 3)).apply(ParDo.of(fn));
    try {
      p.run();
      fail("Pipeline should have failed with an exception");
    } catch (Exception e) {
      assertThat("Function should have been torn down after exception",
          ExceptionThrowingOldFn.teardownCalled.get(),
          is(true));
    }
  }

  private static class ExceptionThrowingOldFn extends DoFn<Object, Object> {
    static AtomicBoolean teardownCalled = new AtomicBoolean(false);

    private final MethodForException toThrow;
    private boolean thrown;

    private ExceptionThrowingOldFn(MethodForException toThrow) {
      this.toThrow = toThrow;
    }

    @Setup
    public void setup() throws Exception {
      throwIfNecessary(MethodForException.SETUP);
    }

    @StartBundle
    public void startBundle() throws Exception {
      throwIfNecessary(MethodForException.START_BUNDLE);
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      throwIfNecessary(MethodForException.PROCESS_ELEMENT);
    }

    @FinishBundle
    public void finishBundle() throws Exception {
      throwIfNecessary(MethodForException.FINISH_BUNDLE);
    }

    private void throwIfNecessary(MethodForException method) throws Exception {
      if (toThrow == method && !thrown) {
        thrown = true;
        throw new Exception("Hasn't yet thrown");
      }
    }

    @Teardown
    public void teardown() {
      if (!thrown) {
        fail("Excepted to have a processing method throw an exception");
      }
      teardownCalled.set(true);
    }
  }


  private static class ExceptionThrowingFn extends DoFn<Object, Object> {
    static AtomicBoolean teardownCalled = new AtomicBoolean(false);

    private final MethodForException toThrow;
    private boolean thrown;

    private ExceptionThrowingFn(MethodForException toThrow) {
      this.toThrow = toThrow;
    }

    @Setup
    public void before() throws Exception {
      throwIfNecessary(MethodForException.SETUP);
    }

    @StartBundle
    public void preBundle() throws Exception {
      throwIfNecessary(MethodForException.START_BUNDLE);
    }

    @ProcessElement
    public void perElement(ProcessContext c) throws Exception {
      throwIfNecessary(MethodForException.PROCESS_ELEMENT);
    }

    @FinishBundle
    public void postBundle() throws Exception {
      throwIfNecessary(MethodForException.FINISH_BUNDLE);
    }

    private void throwIfNecessary(MethodForException method) throws Exception {
      if (toThrow == method && !thrown) {
        thrown = true;
        throw new Exception("Hasn't yet thrown");
      }
    }

    @Teardown
    public void after() {
      if (!thrown) {
        fail("Excepted to have a processing method throw an exception");
      }
      teardownCalled.set(true);
    }
  }

  private enum MethodForException {
    SETUP,
    START_BUNDLE,
    PROCESS_ELEMENT,
    FINISH_BUNDLE
  }
}
