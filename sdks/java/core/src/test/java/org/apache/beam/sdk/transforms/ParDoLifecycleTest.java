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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.DataflowRunnerV2Incompatible;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesParDoLifecycle;
import org.apache.beam.sdk.testing.UsesStatefulParDo;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests that {@link ParDo} exercises {@link DoFn} methods in the appropriate sequence. */
@RunWith(JUnit4.class)
public class ParDoLifecycleTest implements Serializable {

  @Rule public final transient TestPipeline p = TestPipeline.create();

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
  @Category({
    ValidatesRunner.class,
    UsesStatefulParDo.class,
    UsesParDoLifecycle.class,
    DataflowRunnerV2Incompatible.class
  })
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
      assertThat(
          "Even number of startBundle and finishBundle calls in startBundle",
          startBundleCalls,
          equalTo(finishBundleCalls));
      assertThat("teardown should not have been called", teardownCalled, is(false));
      startBundleCalls++;
    }

    @ProcessElement
    public void process(ProcessContext c) throws Exception {
      assertThat("startBundle should have been called", startBundleCalls, greaterThan(0));
      assertThat(
          "there should be one startBundle call with no call to finishBundle",
          startBundleCalls,
          equalTo(finishBundleCalls + 1));
      assertThat("teardown should not have been called", teardownCalled, is(false));
    }

    @FinishBundle
    public void end() {
      assertThat("startBundle should have been called", startBundleCalls, greaterThan(0));
      assertThat(
          "there should be one bundle that has been started but not finished",
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
      extends CallSequenceEnforcingFn<KV<K, V>> {
    private static final String STATE_ID = "foo";

    @StateId(STATE_ID)
    private final StateSpec<ValueState<String>> valueSpec = StateSpecs.value();
  }

  @Test
  @Category({ValidatesRunner.class, UsesParDoLifecycle.class, DataflowRunnerV2Incompatible.class})
  public void testTeardownCalledAfterExceptionInSetup() {
    ExceptionThrowingFn fn = new ExceptionThrowingFn(MethodForException.SETUP);
    p.apply(Create.of(1, 2, 3)).apply(ParDo.of(fn));
    try {
      p.run();
      fail("Pipeline should have failed with an exception");
    } catch (Exception e) {
      validate(CallState.SETUP, CallState.TEARDOWN);
    }
  }

  @Test
  @Category({ValidatesRunner.class, UsesParDoLifecycle.class, DataflowRunnerV2Incompatible.class})
  public void testTeardownCalledAfterExceptionInStartBundle() {
    ExceptionThrowingFn fn = new ExceptionThrowingFn(MethodForException.START_BUNDLE);
    p.apply(Create.of(1, 2, 3)).apply(ParDo.of(fn));
    try {
      p.run();
      fail("Pipeline should have failed with an exception");
    } catch (Exception e) {
      validate(CallState.SETUP, CallState.START_BUNDLE, CallState.TEARDOWN);
    }
  }

  @Test
  @Category({ValidatesRunner.class, UsesParDoLifecycle.class, DataflowRunnerV2Incompatible.class})
  public void testTeardownCalledAfterExceptionInProcessElement() {
    ExceptionThrowingFn fn = new ExceptionThrowingFn(MethodForException.PROCESS_ELEMENT);
    p.apply(Create.of(1, 2, 3)).apply(ParDo.of(fn));
    try {
      p.run();
      fail("Pipeline should have failed with an exception");
    } catch (Exception e) {
      validate(
          CallState.SETUP, CallState.START_BUNDLE, CallState.PROCESS_ELEMENT, CallState.TEARDOWN);
    }
  }

  @Test
  @Category({ValidatesRunner.class, UsesParDoLifecycle.class, DataflowRunnerV2Incompatible.class})
  public void testTeardownCalledAfterExceptionInFinishBundle() {
    ExceptionThrowingFn fn = new ExceptionThrowingFn(MethodForException.FINISH_BUNDLE);
    p.apply(Create.of(1, 2, 3)).apply(ParDo.of(fn));
    try {
      p.run();
      fail("Pipeline should have failed with an exception");
    } catch (Exception e) {
      validate(
          CallState.SETUP,
          CallState.START_BUNDLE,
          CallState.PROCESS_ELEMENT,
          CallState.FINISH_BUNDLE,
          CallState.TEARDOWN);
    }
  }

  @Test
  @Category({
    ValidatesRunner.class,
    UsesStatefulParDo.class,
    UsesParDoLifecycle.class,
    DataflowRunnerV2Incompatible.class
  })
  public void testTeardownCalledAfterExceptionInSetupStateful() {
    ExceptionThrowingFn fn = new ExceptionThrowingStatefulFn(MethodForException.SETUP);
    p.apply(Create.of(KV.of("a", 1), KV.of("b", 2), KV.of("a", 3))).apply(ParDo.of(fn));
    try {
      p.run();
      fail("Pipeline should have failed with an exception");
    } catch (Exception e) {
      validate(CallState.SETUP, CallState.TEARDOWN);
    }
  }

  @Test
  @Category({
    ValidatesRunner.class,
    UsesStatefulParDo.class,
    UsesParDoLifecycle.class,
    DataflowRunnerV2Incompatible.class
  })
  public void testTeardownCalledAfterExceptionInStartBundleStateful() {
    ExceptionThrowingFn fn = new ExceptionThrowingStatefulFn(MethodForException.START_BUNDLE);
    p.apply(Create.of(KV.of("a", 1), KV.of("b", 2), KV.of("a", 3))).apply(ParDo.of(fn));
    try {
      p.run();
      fail("Pipeline should have failed with an exception");
    } catch (Exception e) {
      validate(CallState.SETUP, CallState.START_BUNDLE, CallState.TEARDOWN);
    }
  }

  @Test
  @Category({
    ValidatesRunner.class,
    UsesStatefulParDo.class,
    UsesParDoLifecycle.class,
    DataflowRunnerV2Incompatible.class
  })
  public void testTeardownCalledAfterExceptionInProcessElementStateful() {
    ExceptionThrowingFn fn = new ExceptionThrowingStatefulFn(MethodForException.PROCESS_ELEMENT);
    p.apply(Create.of(KV.of("a", 1), KV.of("b", 2), KV.of("a", 3))).apply(ParDo.of(fn));
    try {
      p.run();
      fail("Pipeline should have failed with an exception");
    } catch (Exception e) {
      validate(
          CallState.SETUP, CallState.START_BUNDLE, CallState.PROCESS_ELEMENT, CallState.TEARDOWN);
    }
  }

  @Test
  @Category({
    ValidatesRunner.class,
    UsesStatefulParDo.class,
    UsesParDoLifecycle.class,
    DataflowRunnerV2Incompatible.class
  })
  public void testTeardownCalledAfterExceptionInFinishBundleStateful() {
    ExceptionThrowingFn fn = new ExceptionThrowingStatefulFn(MethodForException.FINISH_BUNDLE);
    p.apply(Create.of(KV.of("a", 1), KV.of("b", 2), KV.of("a", 3))).apply(ParDo.of(fn));
    try {
      p.run();
      fail("Pipeline should have failed with an exception");
    } catch (Exception e) {
      validate(
          CallState.SETUP,
          CallState.START_BUNDLE,
          CallState.PROCESS_ELEMENT,
          CallState.FINISH_BUNDLE,
          CallState.TEARDOWN);
    }
  }

  private void validate(CallState... requiredCallStates) {
    assertThat(ExceptionThrowingFn.callStateMap, is(not(anEmptyMap())));
    // assert that callStateMap contains only TEARDOWN as a value. Note: We do not expect
    // teardown to be called on fn itself, but on any deserialized instance on which any other
    // lifecycle method was called
    ExceptionThrowingFn.callStateMap
        .values()
        .forEach(
            value ->
                assertThat(
                    "Function should have been torn down after exception",
                    value.finalState(),
                    is(CallState.TEARDOWN)));

    List<CallState> states = Arrays.stream(requiredCallStates).collect(Collectors.toList());
    assertThat(
        "At least one bundle should contain "
            + states
            + ", got "
            + ExceptionThrowingFn.callStateMap.values(),
        ExceptionThrowingFn.callStateMap.values().stream()
            .anyMatch(tracker -> tracker.callStateVisited.equals(states)));
  }

  @Before
  public void setup() {
    ExceptionThrowingFn.callStateMap = new ConcurrentHashMap<>();
    ExceptionThrowingFn.exceptionWasThrown.set(false);
  }

  private static class DelayedCallStateTracker {
    private final CountDownLatch latch;
    private final AtomicReference<CallState> callState;
    private final List<CallState> callStateVisited =
        Collections.synchronizedList(new ArrayList<>());

    private DelayedCallStateTracker(CallState setup) {
      latch = new CountDownLatch(1);
      callState = new AtomicReference<>(setup);
      callStateVisited.add(setup);
    }

    DelayedCallStateTracker update(CallState val) {
      CallState previous = callState.getAndSet(val);
      if (previous == CallState.TEARDOWN && val != CallState.TEARDOWN) {
        fail("illegal state change from " + callState + " to " + val);
      }

      if (CallState.TEARDOWN == val) {
        latch.countDown();
      }
      synchronized (callStateVisited) {
        if (!callStateVisited.contains(val)) {
          callStateVisited.add(val);
        }
      }
      return this;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("latch", latch)
          .add("callState", callState)
          .add("callStateVisited", callStateVisited)
          .toString();
    }

    CallState callState() {
      return callState.get();
    }

    CallState finalState() {
      try {
        // call to tearDown might be delayed on other thread (happens on direct runner)
        // so lets wait a while if not yet called to give a chance to catch up
        latch.await(1, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return callState();
    }
  }

  private static class ExceptionThrowingFn<T> extends DoFn<T, T> {
    static Map<Integer, DelayedCallStateTracker> callStateMap = new ConcurrentHashMap<>();
    // exception is not necessarily thrown on every instance. But we expect at least
    // one during tests
    static AtomicBoolean exceptionWasThrown = new AtomicBoolean(false);
    static AtomicInteger noOfInstancesToTearDown = new AtomicInteger(0);

    private final MethodForException toThrow;
    private boolean thrown;

    private ExceptionThrowingFn(MethodForException toThrow) {
      this.toThrow = toThrow;
    }

    @Setup
    public void before() throws Exception {
      assertThat(
          "lifecycle methods should not have been called", callStateMap.get(id()), is(nullValue()));
      initCallState();
      noOfInstancesToTearDown.incrementAndGet();
      throwIfNecessary(MethodForException.SETUP);
    }

    @StartBundle
    public void preBundle() throws Exception {
      assertThat(
          "lifecycle method should have been called before start bundle",
          getCallState(),
          anyOf(equalTo(CallState.SETUP), equalTo(CallState.FINISH_BUNDLE)));
      updateCallState(CallState.START_BUNDLE);
      throwIfNecessary(MethodForException.START_BUNDLE);
    }

    @ProcessElement
    public void perElement(ProcessContext c) throws Exception {
      assertThat(
          "lifecycle method should have been called before processing bundle",
          getCallState(),
          anyOf(equalTo(CallState.START_BUNDLE), equalTo(CallState.PROCESS_ELEMENT)));
      updateCallState(CallState.PROCESS_ELEMENT);
      throwIfNecessary(MethodForException.PROCESS_ELEMENT);
    }

    @FinishBundle
    public void postBundle() throws Exception {
      assertThat(
          "processing bundle or start bundle should have been called before finish bundle",
          getCallState(),
          anyOf(equalTo(CallState.PROCESS_ELEMENT), equalTo(CallState.START_BUNDLE)));
      updateCallState(CallState.FINISH_BUNDLE);
      throwIfNecessary(MethodForException.FINISH_BUNDLE);
    }

    private void throwIfNecessary(MethodForException method) throws Exception {
      if (toThrow == method && !thrown) {
        thrown = true;
        exceptionWasThrown.set(true);
        throw new Exception("Hasn't yet thrown");
      }
    }

    @Teardown
    public void after() {
      if (noOfInstancesToTearDown.decrementAndGet() == 0 && !exceptionWasThrown.get()) {
        fail("Expected to have a processing method throw an exception");
      }
      assertThat(
          "some lifecycle method should have been called",
          callStateMap.get(id()),
          is(notNullValue()));
      updateCallState(CallState.TEARDOWN);
    }

    private void initCallState() {
      DelayedCallStateTracker previousTracker =
          callStateMap.put(id(), new DelayedCallStateTracker(CallState.SETUP));
      if (previousTracker != null) {
        fail(CallState.SETUP + " method called multiple times");
      }
    }

    private int id() {
      return System.identityHashCode(this);
    }

    private void updateCallState(CallState state) {
      callStateMap.get(id()).update(state);
    }

    private CallState getCallState() {
      return callStateMap.get(id()).callState();
    }
  }

  private static class ExceptionThrowingStatefulFn<K, V> extends ExceptionThrowingFn<KV<K, V>> {
    private static final String STATE_ID = "foo";

    @StateId(STATE_ID)
    private final StateSpec<ValueState<String>> valueSpec = StateSpecs.value();

    private ExceptionThrowingStatefulFn(MethodForException toThrow) {
      super(toThrow);
    }
  }

  private enum CallState {
    SETUP,
    START_BUNDLE,
    PROCESS_ELEMENT,
    FINISH_BUNDLE,
    TEARDOWN
  }

  private enum MethodForException {
    SETUP,
    START_BUNDLE,
    PROCESS_ELEMENT,
    FINISH_BUNDLE
  }
}
