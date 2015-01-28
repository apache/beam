/*
 * Copyright (C) 2014 Google Inc.
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

package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.SUM;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.AnyOf.anyOf;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindows;
import com.google.cloud.dataflow.sdk.util.BatchModeExecutionContext;
import com.google.cloud.dataflow.sdk.util.DoFnInfo;
import com.google.cloud.dataflow.sdk.util.PTuple;
import com.google.cloud.dataflow.sdk.util.UserCodeException;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.Counter;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.Receiver;
import com.google.cloud.dataflow.sdk.values.TupleTag;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tests for NormalParDoFn.
 */
@RunWith(JUnit4.class)
@SuppressWarnings("serial")
public class NormalParDoFnTest {
  static class TestDoFn extends DoFn<Integer, String> {
    enum State { UNSTARTED, STARTED, PROCESSING, FINISHED }
    State state = State.UNSTARTED;

    List<TupleTag> sideOutputTupleTags;

    public TestDoFn(List<String> sideOutputTags) {
      sideOutputTupleTags = new ArrayList<>();
      for (String sideOutputTag : sideOutputTags) {
        sideOutputTupleTags.add(new TupleTag(sideOutputTag));
      }
    }

    @Override
    public void startBundle(Context c) {
      assertEquals(State.UNSTARTED, state);
      state = State.STARTED;
      outputToAll(c, "started");
    }

    @Override
    public void processElement(ProcessContext c) {
      assertThat(state, anyOf(equalTo(State.STARTED),
                              equalTo(State.PROCESSING)));
      state = State.PROCESSING;
      outputToAll(c, "processing: " + c.element());
    }

    @Override
    public void finishBundle(Context c) {
      assertThat(state, anyOf(equalTo(State.STARTED),
                              equalTo(State.PROCESSING)));
      state = State.FINISHED;
      outputToAll(c, "finished");
    }

    private void outputToAll(Context c, String value) {
      c.output(value);
      for (TupleTag sideOutputTupleTag : sideOutputTupleTags) {
        c.sideOutput(sideOutputTupleTag,
                     sideOutputTupleTag.getId() + ": " + value);
      }
    }
  }

  static class TestErrorDoFn extends DoFn<Integer, String> {

    // Used to test nested stack traces.
    private void nestedFunctionBeta(String s) {
      throw new RuntimeException(s);
    }

    private void nestedFunctionAlpha(String s) {
      nestedFunctionBeta(s);
    }

    @Override
    public void startBundle(Context c) {
      nestedFunctionAlpha("test error in initialize");
    }

    @Override
    public void processElement(ProcessContext c) {
      nestedFunctionBeta("test error in process");
    }

    @Override
    public void finishBundle(Context c) {
      throw new RuntimeException("test error in finalize");
    }
  }

  static class TestReceiver implements Receiver {
    List<Object> receivedElems = new ArrayList<>();

    @Override
    public void process(Object outputElem) {
      receivedElems.add(outputElem);
    }
  }

  static class TestDoFnInfoFactory implements NormalParDoFn.DoFnInfoFactory {
    DoFnInfo fnInfo;

    TestDoFnInfoFactory(DoFnInfo fnInfo) {
      this.fnInfo = fnInfo;
    }

    public DoFnInfo createDoFnInfo() {
      return fnInfo;
    }
  }

  @Test
  public void testNormalParDoFn() throws Exception {
    List<String> sideOutputTags = Arrays.asList("tag1", "tag2", "tag3");

    TestDoFn fn = new TestDoFn(sideOutputTags);
    DoFnInfo fnInfo = new DoFnInfo(fn, new GlobalWindows());
    TestReceiver receiver = new TestReceiver();
    TestReceiver receiver1 = new TestReceiver();
    TestReceiver receiver2 = new TestReceiver();
    TestReceiver receiver3 = new TestReceiver();

    PTuple sideInputValues = PTuple.empty();

    List<String> outputTags = new ArrayList<>();
    outputTags.add("output");
    outputTags.addAll(sideOutputTags);
    NormalParDoFn normalParDoFn =
        new NormalParDoFn(PipelineOptionsFactory.create(),
                          new TestDoFnInfoFactory(fnInfo), sideInputValues, outputTags, "doFn",
                          new BatchModeExecutionContext(),
                          (new CounterSet()).getAddCounterMutator());

    normalParDoFn.startBundle(receiver, receiver1, receiver2, receiver3);

    normalParDoFn.processElement(WindowedValue.valueInGlobalWindow(3));
    normalParDoFn.processElement(WindowedValue.valueInGlobalWindow(42));
    normalParDoFn.processElement(WindowedValue.valueInGlobalWindow(666));

    normalParDoFn.finishBundle();

    Object[] expectedReceivedElems = {
      WindowedValue.valueInGlobalWindow("started"),
      WindowedValue.valueInGlobalWindow("processing: 3"),
      WindowedValue.valueInGlobalWindow("processing: 42"),
      WindowedValue.valueInGlobalWindow("processing: 666"),
      WindowedValue.valueInGlobalWindow("finished"),
    };
    assertArrayEquals(expectedReceivedElems, receiver.receivedElems.toArray());

    Object[] expectedReceivedElems1 = {
      WindowedValue.valueInGlobalWindow("tag1: started"),
      WindowedValue.valueInGlobalWindow("tag1: processing: 3"),
      WindowedValue.valueInGlobalWindow("tag1: processing: 42"),
      WindowedValue.valueInGlobalWindow("tag1: processing: 666"),
      WindowedValue.valueInGlobalWindow("tag1: finished"),
    };
    assertArrayEquals(expectedReceivedElems1, receiver1.receivedElems.toArray());

    Object[] expectedReceivedElems2 = {
      WindowedValue.valueInGlobalWindow("tag2: started"),
      WindowedValue.valueInGlobalWindow("tag2: processing: 3"),
      WindowedValue.valueInGlobalWindow("tag2: processing: 42"),
      WindowedValue.valueInGlobalWindow("tag2: processing: 666"),
      WindowedValue.valueInGlobalWindow("tag2: finished"),
    };
    assertArrayEquals(expectedReceivedElems2, receiver2.receivedElems.toArray());

    Object[] expectedReceivedElems3 = {
      WindowedValue.valueInGlobalWindow("tag3: started"),
      WindowedValue.valueInGlobalWindow("tag3: processing: 3"),
      WindowedValue.valueInGlobalWindow("tag3: processing: 42"),
      WindowedValue.valueInGlobalWindow("tag3: processing: 666"),
      WindowedValue.valueInGlobalWindow("tag3: finished"),
    };
    assertArrayEquals(expectedReceivedElems3, receiver3.receivedElems.toArray());
  }

  @Test
  public void testUnexpectedNumberOfReceivers() throws Exception {
    TestDoFn fn = new TestDoFn(Collections.<String>emptyList());
    DoFnInfo fnInfo = new DoFnInfo(fn, new GlobalWindows());
    TestReceiver receiver = new TestReceiver();

    PTuple sideInputValues = PTuple.empty();
    List<String> outputTags = Arrays.asList("output");
    NormalParDoFn normalParDoFn =
        new NormalParDoFn(PipelineOptionsFactory.create(),
                          new TestDoFnInfoFactory(fnInfo), sideInputValues, outputTags, "doFn",
                          new BatchModeExecutionContext(),
                          (new CounterSet()).getAddCounterMutator());

    try {
      normalParDoFn.startBundle();
      fail("should have failed");
    } catch (Throwable exn) {
      assertThat(exn.toString(),
                 containsString("unexpected number of receivers"));
    }
    try {
      normalParDoFn.startBundle(receiver, receiver);
      fail("should have failed");
    } catch (Throwable exn) {
      assertThat(exn.toString(),
                 containsString("unexpected number of receivers"));
    }
  }

  private List<String> stackTraceFrameStrings(Throwable t) {
    List<String> stack = new ArrayList<>();
    for (StackTraceElement frame : t.getStackTrace()) {
      // Make sure that the frame has the expected name.
      stack.add(frame.toString());
    }
    return stack;
  }

  @Test
  public void testErrorPropagation() throws Exception {
    TestErrorDoFn fn = new TestErrorDoFn();
    DoFnInfo fnInfo = new DoFnInfo(fn, new GlobalWindows());
    TestReceiver receiver = new TestReceiver();

    PTuple sideInputValues = PTuple.empty();
    List<String> outputTags = Arrays.asList("output");
    NormalParDoFn normalParDoFn =
        new NormalParDoFn(PipelineOptionsFactory.create(),
                          new TestDoFnInfoFactory(fnInfo), sideInputValues, outputTags, "doFn",
                          new BatchModeExecutionContext(),
                          (new CounterSet()).getAddCounterMutator());

    try {
      normalParDoFn.startBundle(receiver);
      fail("should have failed");
    } catch (Exception exn) {
      // Because we're calling this from inside the SDK and not from a
      // user's program (e.g. through Pipeline.run), the error should
      // be thrown as a UserCodeException. The cause of the
      // UserCodeError shouldn't contain any of the stack from within
      // the SDK, since we don't want to overwhelm users with stack
      // frames outside of their control.
      assertThat(exn, instanceOf(UserCodeException.class));
      // Stack trace of the cause should contain three frames:
      // TestErrorDoFn.nestedFunctionBeta
      // TestErrorDoFn.nestedFunctionAlpha
      // TestErrorDoFn.startBundle
      assertThat(stackTraceFrameStrings(exn.getCause()), contains(
          containsString("TestErrorDoFn.nestedFunctionBeta"),
          containsString("TestErrorDoFn.nestedFunctionAlpha"),
          containsString("TestErrorDoFn.startBundle")));
      assertThat(exn.toString(),
                 containsString("test error in initialize"));
    }

    try {
      normalParDoFn.processElement(WindowedValue.valueInGlobalWindow(3));
      fail("should have failed");
    } catch (Exception exn) {
      // Exception should be a UserCodeException since we're calling
      // from inside the SDK.
      assertThat(exn, instanceOf(UserCodeException.class));
      // Stack trace of the cause should contain two frames:
      // TestErrorDoFn.nestedFunctionBeta
      // TestErrorDoFn.processElement
      assertThat(stackTraceFrameStrings(exn.getCause()), contains(
          containsString("TestErrorDoFn.nestedFunctionBeta"),
          containsString("TestErrorDoFn.processElement")));
      assertThat(exn.toString(), containsString("test error in process"));
    }

    try {
      normalParDoFn.finishBundle();
      fail("should have failed");
    } catch (Exception exn) {
      // Exception should be a UserCodeException since we're calling
      // from inside the SDK.
      assertThat(exn, instanceOf(UserCodeException.class));
      // Stack trace should only contain a single frame:
      // TestErrorDoFn.finishBundle
      assertThat(stackTraceFrameStrings(exn.getCause()), contains(
          containsString("TestErrorDoFn.finishBundle")));
      assertThat(exn.toString(), containsString("test error in finalize"));
    }
  }

  @Test
  public void testUndeclaredSideOutputs() throws Exception {
    TestDoFn fn = new TestDoFn(Arrays.asList("declared", "undecl1", "undecl2", "undecl3"));
    DoFnInfo fnInfo = new DoFnInfo(fn, new GlobalWindows());
    CounterSet counters = new CounterSet();
    NormalParDoFn normalParDoFn =
        new NormalParDoFn(
            PipelineOptionsFactory.create(), new TestDoFnInfoFactory(fnInfo), PTuple.empty(),
            Arrays.asList("output", "declared"), "doFn",
            new BatchModeExecutionContext(),
            counters.getAddCounterMutator());

    normalParDoFn.startBundle(new TestReceiver(), new TestReceiver());
    normalParDoFn.processElement(WindowedValue.valueInGlobalWindow(5));
    normalParDoFn.finishBundle();

    assertEquals(
        new CounterSet(
            Counter.longs("implicit-undecl1-ElementCount", SUM)
            .resetToValue(3L),
            Counter.longs("implicit-undecl2-ElementCount", SUM)
            .resetToValue(3L),
            Counter.longs("implicit-undecl3-ElementCount", SUM)
            .resetToValue(3L)),
        counters);
  }
}
