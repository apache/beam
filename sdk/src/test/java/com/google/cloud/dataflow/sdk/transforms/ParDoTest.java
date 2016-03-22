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

package com.google.cloud.dataflow.sdk.transforms;

import static com.google.cloud.dataflow.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static com.google.cloud.dataflow.sdk.transforms.display.DisplayDataMatchers.hasKey;
import static com.google.cloud.dataflow.sdk.util.SerializableUtils.serializeToByteArray;
import static com.google.cloud.dataflow.sdk.util.StringUtils.byteArrayToJsonString;
import static com.google.cloud.dataflow.sdk.util.StringUtils.jsonStringToByteArray;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.AnyOf.anyOf;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.Pipeline.PipelineExecutionException;
import com.google.cloud.dataflow.sdk.coders.AtomicCoder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.ListCoder;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.RunnableOnService;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.DoFn.RequiresWindowAccess;
import com.google.cloud.dataflow.sdk.transforms.ParDo.Bound;
import com.google.cloud.dataflow.sdk.transforms.display.DisplayData;
import com.google.cloud.dataflow.sdk.transforms.display.DisplayData.Builder;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.util.IllegalMutationException;
import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObserver;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TimestampedValue;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.dataflow.sdk.values.TupleTagList;
import com.google.common.base.Preconditions;

import com.fasterxml.jackson.annotation.JsonCreator;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tests for ParDo.
 */
@RunWith(JUnit4.class)
public class ParDoTest implements Serializable {
  // This test is Serializable, just so that it's easy to have
  // anonymous inner classes inside the non-static test methods.

  @Rule
  public transient ExpectedException thrown = ExpectedException.none();

  private static class PrintingDoFn extends DoFn<String, String> implements RequiresWindowAccess {
    @Override
    public void processElement(ProcessContext c) {
      c.output(c.element() + ":" + c.timestamp().getMillis()
          + ":" + c.window().maxTimestamp().getMillis());
    }
  }

  static class TestDoFn extends DoFn<Integer, String> {
    enum State { UNSTARTED, STARTED, PROCESSING, FINISHED }
    State state = State.UNSTARTED;

    final List<PCollectionView<Integer>> sideInputViews = new ArrayList<>();
    final List<TupleTag<String>> sideOutputTupleTags = new ArrayList<>();

    public TestDoFn() {
    }

    public TestDoFn(List<PCollectionView<Integer>> sideInputViews,
                    List<TupleTag<String>> sideOutputTupleTags) {
      this.sideInputViews.addAll(sideInputViews);
      this.sideOutputTupleTags.addAll(sideOutputTupleTags);
    }

    @Override
    public void startBundle(Context c) {
      assertEquals(State.UNSTARTED, state);
      state = State.STARTED;
      outputToAll(c, "started");
    }

    @Override
    public void processElement(ProcessContext c) {
      assertThat(state,
                 anyOf(equalTo(State.STARTED), equalTo(State.PROCESSING)));
      state = State.PROCESSING;
      outputToAllWithSideInputs(c, "processing: " + c.element());
    }

    @Override
    public void finishBundle(Context c) {
      assertThat(state,
                 anyOf(equalTo(State.STARTED), equalTo(State.PROCESSING)));
      state = State.FINISHED;
      outputToAll(c, "finished");
    }

    private void outputToAll(Context c, String value) {
      c.output(value);
      for (TupleTag<String> sideOutputTupleTag : sideOutputTupleTags) {
        c.sideOutput(sideOutputTupleTag,
                     sideOutputTupleTag.getId() + ": " + value);
      }
    }

    private void outputToAllWithSideInputs(ProcessContext c, String value) {
      if (!sideInputViews.isEmpty()) {
        List<Integer> sideInputValues = new ArrayList<>();
        for (PCollectionView<Integer> sideInputView : sideInputViews) {
          sideInputValues.add(c.sideInput(sideInputView));
        }
        value += ": " + sideInputValues;
      }
      c.output(value);
      for (TupleTag<String> sideOutputTupleTag : sideOutputTupleTags) {
        c.sideOutput(sideOutputTupleTag,
                     sideOutputTupleTag.getId() + ": " + value);
      }
    }
  }

  static class TestNoOutputDoFn extends DoFn<Integer, String> {
    @Override
    public void processElement(DoFn<Integer, String>.ProcessContext c) throws Exception {}
  }

  static class TestDoFnWithContext extends DoFnWithContext<Integer, String> {
    enum State { UNSTARTED, STARTED, PROCESSING, FINISHED }
    State state = State.UNSTARTED;

    final List<PCollectionView<Integer>> sideInputViews = new ArrayList<>();
    final List<TupleTag<String>> sideOutputTupleTags = new ArrayList<>();

    public TestDoFnWithContext() {
    }

    public TestDoFnWithContext(List<PCollectionView<Integer>> sideInputViews,
                    List<TupleTag<String>> sideOutputTupleTags) {
      this.sideInputViews.addAll(sideInputViews);
      this.sideOutputTupleTags.addAll(sideOutputTupleTags);
    }

    @StartBundle
    public void startBundle(Context c) {
      assertEquals(State.UNSTARTED, state);
      state = State.STARTED;
      outputToAll(c, "started");
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      assertThat(state,
                 anyOf(equalTo(State.STARTED), equalTo(State.PROCESSING)));
      state = State.PROCESSING;
      outputToAllWithSideInputs(c, "processing: " + c.element());
    }

    @FinishBundle
    public void finishBundle(Context c) {
      assertThat(state,
                 anyOf(equalTo(State.STARTED), equalTo(State.PROCESSING)));
      state = State.FINISHED;
      outputToAll(c, "finished");
    }

    private void outputToAll(Context c, String value) {
      c.output(value);
      for (TupleTag<String> sideOutputTupleTag : sideOutputTupleTags) {
        c.sideOutput(sideOutputTupleTag,
                     sideOutputTupleTag.getId() + ": " + value);
      }
    }

    private void outputToAllWithSideInputs(ProcessContext c, String value) {
      if (!sideInputViews.isEmpty()) {
        List<Integer> sideInputValues = new ArrayList<>();
        for (PCollectionView<Integer> sideInputView : sideInputViews) {
          sideInputValues.add(c.sideInput(sideInputView));
        }
        value += ": " + sideInputValues;
      }
      c.output(value);
      for (TupleTag<String> sideOutputTupleTag : sideOutputTupleTags) {
        c.sideOutput(sideOutputTupleTag,
                     sideOutputTupleTag.getId() + ": " + value);
      }
    }
  }

  static class TestStartBatchErrorDoFn extends DoFn<Integer, String> {
    @Override
    public void startBundle(Context c) {
      throw new RuntimeException("test error in initialize");
    }

    @Override
    public void processElement(ProcessContext c) {
      // This has to be here.
    }
  }

  static class TestProcessElementErrorDoFn extends DoFn<Integer, String> {
    @Override
    public void processElement(ProcessContext c) {
      throw new RuntimeException("test error in process");
    }
  }

  static class TestFinishBatchErrorDoFn extends DoFn<Integer, String> {
    @Override
    public void processElement(ProcessContext c) {
      // This has to be here.
    }

    @Override
    public void finishBundle(Context c) {
      throw new RuntimeException("test error in finalize");
    }
  }

  private static class StrangelyNamedDoer extends DoFn<Integer, String> {
    @Override
    public void processElement(ProcessContext c) {
    }
  }

  static class TestOutputTimestampDoFn extends DoFn<Integer, Integer> {
    @Override
    public void processElement(ProcessContext c) {
      Integer value = c.element();
      c.outputWithTimestamp(value, new Instant(value.longValue()));
    }
  }

  static class TestShiftTimestampDoFn extends DoFn<Integer, Integer> {
    private Duration allowedTimestampSkew;
    private Duration durationToShift;

    public TestShiftTimestampDoFn(Duration allowedTimestampSkew,
                                  Duration durationToShift) {
      this.allowedTimestampSkew = allowedTimestampSkew;
      this.durationToShift = durationToShift;
    }

    @Override
    public Duration getAllowedTimestampSkew() {
      return allowedTimestampSkew;
    }
    @Override
    public void processElement(ProcessContext c) {
      Instant timestamp = c.timestamp();
      Preconditions.checkNotNull(timestamp);
      Integer value = c.element();
      c.outputWithTimestamp(value, timestamp.plus(durationToShift));
    }
  }

  static class TestFormatTimestampDoFn extends DoFn<Integer, String> {
    @Override
    public void processElement(ProcessContext c) {
      Preconditions.checkNotNull(c.timestamp());
      c.output("processing: " + c.element() + ", timestamp: " + c.timestamp().getMillis());
    }
  }

  static class MultiFilter
      extends PTransform<PCollection<Integer>, PCollectionTuple> {

    private static final TupleTag<Integer> BY2 = new TupleTag<Integer>("by2"){};
    private static final TupleTag<Integer> BY3 = new TupleTag<Integer>("by3"){};

    @Override
    public PCollectionTuple apply(PCollection<Integer> input) {
      PCollection<Integer> by2 = input.apply("Filter2s", ParDo.of(new FilterFn(2)));
      PCollection<Integer> by3 = input.apply("Filter3s", ParDo.of(new FilterFn(3)));
      return PCollectionTuple.of(BY2, by2).and(BY3, by3);
    }

    static class FilterFn extends DoFn<Integer, Integer> {
      private final int divisor;

      FilterFn(int divisor) {
        this.divisor = divisor;
      }

      @Override
      public void processElement(ProcessContext c) throws Exception {
        if (c.element() % divisor == 0) {
          c.output(c.element());
        }
      }
    }
  }

  @Test
  @Category(RunnableOnService.class)
  public void testParDo() {
    Pipeline pipeline = TestPipeline.create();

    List<Integer> inputs = Arrays.asList(3, -42, 666);

    PCollection<String> output = pipeline
        .apply(Create.of(inputs))
        .apply(ParDo.of(new TestDoFn()));

    DataflowAssert.that(output)
        .satisfies(ParDoTest.HasExpectedOutput.forInput(inputs));

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testParDo2() {
    Pipeline pipeline = TestPipeline.create();

    List<Integer> inputs = Arrays.asList(3, -42, 666);

    PCollection<String> output = pipeline
        .apply(Create.of(inputs))
        .apply(ParDo.of(new TestDoFnWithContext()));

    DataflowAssert.that(output)
        .satisfies(ParDoTest.HasExpectedOutput.forInput(inputs));

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testParDoEmpty() {
    Pipeline pipeline = TestPipeline.create();

    List<Integer> inputs = Arrays.asList();

    PCollection<String> output = pipeline
        .apply(Create.of(inputs).withCoder(VarIntCoder.of()))
        .apply("TestDoFn", ParDo.of(new TestDoFn()));

    DataflowAssert.that(output)
        .satisfies(ParDoTest.HasExpectedOutput.forInput(inputs));

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testParDoEmptyOutputs() {

    Pipeline pipeline = TestPipeline.create();

    List<Integer> inputs = Arrays.asList();

    PCollection<String> output = pipeline
        .apply(Create.of(inputs).withCoder(VarIntCoder.of()))
        .apply("TestDoFn", ParDo.of(new TestNoOutputDoFn()));

    DataflowAssert.that(output).empty();

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testParDoWithSideOutputs() {
    Pipeline pipeline = TestPipeline.create();

    List<Integer> inputs = Arrays.asList(3, -42, 666);

    TupleTag<String> mainOutputTag = new TupleTag<String>("main"){};
    TupleTag<String> sideOutputTag1 = new TupleTag<String>("side1"){};
    TupleTag<String> sideOutputTag2 = new TupleTag<String>("side2"){};
    TupleTag<String> sideOutputTag3 = new TupleTag<String>("side3"){};
    TupleTag<String> sideOutputTagUnwritten = new TupleTag<String>("sideUnwritten"){};

    PCollectionTuple outputs = pipeline
        .apply(Create.of(inputs))
        .apply(ParDo
               .of(new TestDoFn(
                   Arrays.<PCollectionView<Integer>>asList(),
                   Arrays.asList(sideOutputTag1, sideOutputTag2, sideOutputTag3)))
               .withOutputTags(
                   mainOutputTag,
                   TupleTagList.of(sideOutputTag3)
                       .and(sideOutputTag1)
                       .and(sideOutputTagUnwritten)
                       .and(sideOutputTag2)));

    DataflowAssert.that(outputs.get(mainOutputTag))
        .satisfies(ParDoTest.HasExpectedOutput.forInput(inputs));

    DataflowAssert.that(outputs.get(sideOutputTag1))
        .satisfies(ParDoTest.HasExpectedOutput.forInput(inputs)
                   .fromSideOutput(sideOutputTag1));
    DataflowAssert.that(outputs.get(sideOutputTag2))
        .satisfies(ParDoTest.HasExpectedOutput.forInput(inputs)
                   .fromSideOutput(sideOutputTag2));
    DataflowAssert.that(outputs.get(sideOutputTag3))
        .satisfies(ParDoTest.HasExpectedOutput.forInput(inputs)
                   .fromSideOutput(sideOutputTag3));
    DataflowAssert.that(outputs.get(sideOutputTagUnwritten)).empty();

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testParDoEmptyWithSideOutputs() {
    Pipeline pipeline = TestPipeline.create();

    List<Integer> inputs = Arrays.asList();

    TupleTag<String> mainOutputTag = new TupleTag<String>("main"){};
    TupleTag<String> sideOutputTag1 = new TupleTag<String>("side1"){};
    TupleTag<String> sideOutputTag2 = new TupleTag<String>("side2"){};
    TupleTag<String> sideOutputTag3 = new TupleTag<String>("side3"){};
    TupleTag<String> sideOutputTagUnwritten = new TupleTag<String>("sideUnwritten"){};

    PCollectionTuple outputs = pipeline
        .apply(Create.of(inputs))
        .apply(ParDo
               .of(new TestDoFn(
                   Arrays.<PCollectionView<Integer>>asList(),
                   Arrays.asList(sideOutputTag1, sideOutputTag2, sideOutputTag3)))
               .withOutputTags(
                   mainOutputTag,
                   TupleTagList.of(sideOutputTag3).and(sideOutputTag1)
                   .and(sideOutputTagUnwritten).and(sideOutputTag2)));

    DataflowAssert.that(outputs.get(mainOutputTag))
        .satisfies(ParDoTest.HasExpectedOutput.forInput(inputs));

    DataflowAssert.that(outputs.get(sideOutputTag1))
        .satisfies(ParDoTest.HasExpectedOutput.forInput(inputs)
                   .fromSideOutput(sideOutputTag1));
    DataflowAssert.that(outputs.get(sideOutputTag2))
        .satisfies(ParDoTest.HasExpectedOutput.forInput(inputs)
                   .fromSideOutput(sideOutputTag2));
    DataflowAssert.that(outputs.get(sideOutputTag3))
        .satisfies(ParDoTest.HasExpectedOutput.forInput(inputs)
                   .fromSideOutput(sideOutputTag3));
    DataflowAssert.that(outputs.get(sideOutputTagUnwritten)).empty();

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testParDoWithEmptySideOutputs() {
    Pipeline pipeline = TestPipeline.create();

    List<Integer> inputs = Arrays.asList();

    TupleTag<String> mainOutputTag = new TupleTag<String>("main"){};
    TupleTag<String> sideOutputTag1 = new TupleTag<String>("side1"){};
    TupleTag<String> sideOutputTag2 = new TupleTag<String>("side2"){};

    PCollectionTuple outputs = pipeline
        .apply(Create.of(inputs))
        .apply(ParDo
               .of(new TestNoOutputDoFn())
               .withOutputTags(
                   mainOutputTag,
                   TupleTagList.of(sideOutputTag1).and(sideOutputTag2)));

    DataflowAssert.that(outputs.get(mainOutputTag)).empty();

    DataflowAssert.that(outputs.get(sideOutputTag1)).empty();
    DataflowAssert.that(outputs.get(sideOutputTag2)).empty();

    pipeline.run();
  }


  @Test
  @Category(RunnableOnService.class)
  public void testParDoWithOnlySideOutputs() {
    Pipeline pipeline = TestPipeline.create();

    List<Integer> inputs = Arrays.asList(3, -42, 666);

    final TupleTag<Void> mainOutputTag = new TupleTag<Void>("main"){};
    final TupleTag<Integer> sideOutputTag = new TupleTag<Integer>("side"){};

    PCollectionTuple outputs = pipeline
        .apply(Create.of(inputs))
        .apply(ParDo.withOutputTags(mainOutputTag, TupleTagList.of(sideOutputTag))
            .of(new DoFn<Integer, Void>(){
                @Override
                public void processElement(ProcessContext c) {
                  c.sideOutput(sideOutputTag, c.element());
                }}));

    DataflowAssert.that(outputs.get(mainOutputTag)).empty();
    DataflowAssert.that(outputs.get(sideOutputTag)).containsInAnyOrder(inputs);

    pipeline.run();
  }

  @Test
  public void testParDoWritingToUndeclaredSideOutput() {
    Pipeline pipeline = TestPipeline.create();

    List<Integer> inputs = Arrays.asList(3, -42, 666);

    TupleTag<String> sideTag = new TupleTag<String>("side"){};

    PCollection<String> output = pipeline
        .apply(Create.of(inputs))
        .apply(ParDo.of(new TestDoFn(
            Arrays.<PCollectionView<Integer>>asList(),
            Arrays.asList(sideTag))));

    DataflowAssert.that(output)
        .satisfies(ParDoTest.HasExpectedOutput.forInput(inputs));

    pipeline.run();
  }

  @Test
  public void testParDoUndeclaredSideOutputLimit() {
    Pipeline pipeline = TestPipeline.create();
    PCollection<Integer> input = pipeline.apply(Create.of(Arrays.asList(3)));

    // Success for a total of 1000 outputs.
    input
        .apply("Success1000", ParDo.of(new DoFn<Integer, String>() {
            @Override
            public void processElement(ProcessContext c) {
              TupleTag<String> specialSideTag = new TupleTag<String>(){};
              c.sideOutput(specialSideTag, "side");
              c.sideOutput(specialSideTag, "side");
              c.sideOutput(specialSideTag, "side");

              for (int i = 0; i < 998; i++) {
                c.sideOutput(new TupleTag<String>(){}, "side");
              }
            }}));
    pipeline.run();

    // Failure for a total of 1001 outputs.
    input
        .apply("Failure1001", ParDo.of(new DoFn<Integer, String>() {
            @Override
            public void processElement(ProcessContext c) {
              for (int i = 0; i < 1000; i++) {
                c.sideOutput(new TupleTag<String>(){}, "side");
              }
            }}));

    thrown.expect(RuntimeException.class);
    thrown.expectMessage("the number of side outputs has exceeded a limit");
    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testParDoWithSideInputs() {
    Pipeline pipeline = TestPipeline.create();

    List<Integer> inputs = Arrays.asList(3, -42, 666);

    PCollectionView<Integer> sideInput1 = pipeline
        .apply("CreateSideInput1", Create.of(11))
        .apply("ViewSideInput1", View.<Integer>asSingleton());
    PCollectionView<Integer> sideInputUnread = pipeline
        .apply("CreateSideInputUnread", Create.of(-3333))
        .apply("ViewSideInputUnread", View.<Integer>asSingleton());
    PCollectionView<Integer> sideInput2 = pipeline
        .apply("CreateSideInput2", Create.of(222))
        .apply("ViewSideInput2", View.<Integer>asSingleton());

    PCollection<String> output = pipeline
        .apply(Create.of(inputs))
        .apply(ParDo.withSideInputs(sideInput1, sideInputUnread, sideInput2)
            .of(new TestDoFn(
                Arrays.asList(sideInput1, sideInput2),
                Arrays.<TupleTag<String>>asList())));

    DataflowAssert.that(output)
        .satisfies(ParDoTest.HasExpectedOutput
                   .forInput(inputs)
                   .andSideInputs(11, 222));

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testParDoWithSideInputsIsCumulative() {
    Pipeline pipeline = TestPipeline.create();

    List<Integer> inputs = Arrays.asList(3, -42, 666);

    PCollectionView<Integer> sideInput1 = pipeline
        .apply("CreateSideInput1", Create.of(11))
        .apply("ViewSideInput1", View.<Integer>asSingleton());
    PCollectionView<Integer> sideInputUnread = pipeline
        .apply("CreateSideInputUnread", Create.of(-3333))
        .apply("ViewSideInputUnread", View.<Integer>asSingleton());
    PCollectionView<Integer> sideInput2 = pipeline
        .apply("CreateSideInput2", Create.of(222))
        .apply("ViewSideInput2", View.<Integer>asSingleton());

    PCollection<String> output = pipeline
        .apply(Create.of(inputs))
        .apply(ParDo.withSideInputs(sideInput1)
            .withSideInputs(sideInputUnread)
            .withSideInputs(sideInput2)
            .of(new TestDoFn(
                Arrays.asList(sideInput1, sideInput2),
                Arrays.<TupleTag<String>>asList())));

    DataflowAssert.that(output)
        .satisfies(ParDoTest.HasExpectedOutput
                   .forInput(inputs)
                   .andSideInputs(11, 222));

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testMultiOutputParDoWithSideInputs() {
    Pipeline pipeline = TestPipeline.create();

    List<Integer> inputs = Arrays.asList(3, -42, 666);

    final TupleTag<String> mainOutputTag = new TupleTag<String>("main"){};
    final TupleTag<Void> sideOutputTag = new TupleTag<Void>("sideOutput"){};

    PCollectionView<Integer> sideInput1 = pipeline
        .apply("CreateSideInput1", Create.of(11))
        .apply("ViewSideInput1", View.<Integer>asSingleton());
    PCollectionView<Integer> sideInputUnread = pipeline
        .apply("CreateSideInputUnread", Create.of(-3333))
        .apply("ViewSideInputUnread", View.<Integer>asSingleton());
    PCollectionView<Integer> sideInput2 = pipeline
        .apply("CreateSideInput2", Create.of(222))
        .apply("ViewSideInput2", View.<Integer>asSingleton());

    PCollectionTuple outputs = pipeline
        .apply(Create.of(inputs))
        .apply(ParDo.withSideInputs(sideInput1)
            .withSideInputs(sideInputUnread)
            .withSideInputs(sideInput2)
            .withOutputTags(mainOutputTag, TupleTagList.of(sideOutputTag))
            .of(new TestDoFn(
                Arrays.asList(sideInput1, sideInput2),
                Arrays.<TupleTag<String>>asList())));

    DataflowAssert.that(outputs.get(mainOutputTag))
        .satisfies(ParDoTest.HasExpectedOutput
                   .forInput(inputs)
                   .andSideInputs(11, 222));

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testMultiOutputParDoWithSideInputsIsCumulative() {
    Pipeline pipeline = TestPipeline.create();

    List<Integer> inputs = Arrays.asList(3, -42, 666);

    final TupleTag<String> mainOutputTag = new TupleTag<String>("main"){};
    final TupleTag<Void> sideOutputTag = new TupleTag<Void>("sideOutput"){};

    PCollectionView<Integer> sideInput1 = pipeline
        .apply("CreateSideInput1", Create.of(11))
        .apply("ViewSideInput1", View.<Integer>asSingleton());
    PCollectionView<Integer> sideInputUnread = pipeline
        .apply("CreateSideInputUnread", Create.of(-3333))
        .apply("ViewSideInputUnread", View.<Integer>asSingleton());
    PCollectionView<Integer> sideInput2 = pipeline
        .apply("CreateSideInput2", Create.of(222))
        .apply("ViewSideInput2", View.<Integer>asSingleton());

    PCollectionTuple outputs = pipeline
        .apply(Create.of(inputs))
        .apply(ParDo.withSideInputs(sideInput1)
            .withSideInputs(sideInputUnread)
            .withSideInputs(sideInput2)
            .withOutputTags(mainOutputTag, TupleTagList.of(sideOutputTag))
            .of(new TestDoFn(
                Arrays.asList(sideInput1, sideInput2),
                Arrays.<TupleTag<String>>asList())));

    DataflowAssert.that(outputs.get(mainOutputTag))
        .satisfies(ParDoTest.HasExpectedOutput
                   .forInput(inputs)
                   .andSideInputs(11, 222));

    pipeline.run();
  }

  @Test
  public void testParDoReadingFromUnknownSideInput() {
    Pipeline pipeline = TestPipeline.create();

    List<Integer> inputs = Arrays.asList(3, -42, 666);

    PCollectionView<Integer> sideView = pipeline
        .apply("Create3", Create.of(3))
        .apply(View.<Integer>asSingleton());

    pipeline.apply("CreateMain", Create.of(inputs))
        .apply(ParDo.of(new TestDoFn(
            Arrays.<PCollectionView<Integer>>asList(sideView),
            Arrays.<TupleTag<String>>asList())));

    thrown.expect(RuntimeException.class);
    thrown.expectMessage("calling sideInput() with unknown view");
    pipeline.run();
  }

  @Test
  public void testParDoWithErrorInStartBatch() {
    Pipeline pipeline = TestPipeline.create();

    List<Integer> inputs = Arrays.asList(3, -42, 666);

    pipeline.apply(Create.of(inputs))
        .apply(ParDo.of(new TestStartBatchErrorDoFn()));

    thrown.expect(RuntimeException.class);
    thrown.expectMessage("test error in initialize");
    pipeline.run();
  }

  @Test
  public void testParDoWithErrorInProcessElement() {
    Pipeline pipeline = TestPipeline.create();

    List<Integer> inputs = Arrays.asList(3, -42, 666);

    pipeline.apply(Create.of(inputs))
        .apply(ParDo.of(new TestProcessElementErrorDoFn()));

    thrown.expect(RuntimeException.class);
    thrown.expectMessage("test error in process");
    pipeline.run();
  }

  @Test
  public void testParDoWithErrorInFinishBatch() {
    Pipeline pipeline = TestPipeline.create();

    List<Integer> inputs = Arrays.asList(3, -42, 666);

    pipeline.apply(Create.of(inputs))
        .apply(ParDo.of(new TestFinishBatchErrorDoFn()));

    thrown.expect(RuntimeException.class);
    thrown.expectMessage("test error in finalize");
    pipeline.run();
  }

  @Test
  public void testParDoGetName() {
    Pipeline p = TestPipeline.create();

    PCollection<Integer> input =
        p.apply(Create.of(Arrays.asList(3, -42, 666)))
        .setName("MyInput");

    {
      PCollection<String> output1 =
          input
          .apply(ParDo.of(new TestDoFn()));
      assertEquals("ParDo(Test).out", output1.getName());
    }

    {
      PCollection<String> output2 =
          input
          .apply(ParDo.named("MyParDo").of(new TestDoFn()));
      assertEquals("MyParDo.out", output2.getName());
    }

    {
      PCollection<String> output3 =
          input
          .apply(ParDo.of(new TestDoFn()).named("HerParDo"));
      assertEquals("HerParDo.out", output3.getName());
    }

    {
      PCollection<String> output4 =
          input
              .apply(ParDo.of(new TestDoFn()).named("TestDoFn"));
      assertEquals("TestDoFn.out", output4.getName());
    }

    {
      PCollection<String> output5 =
          input
              .apply(ParDo.of(new StrangelyNamedDoer()));
      assertEquals("ParDo(StrangelyNamedDoer).out",
          output5.getName());
    }

    assertEquals("ParDo(Printing)", ParDo.of(new PrintingDoFn()).getName());

    assertEquals(
        "ParMultiDo(SideOutputDummy)",
        ParDo.of(new SideOutputDummyFn(null)).withOutputTags(null, null).getName());
  }

  @Test
  public void testParDoWithSideOutputsName() {
    Pipeline p = TestPipeline.create();

    TupleTag<String> mainOutputTag = new TupleTag<String>("main"){};
    TupleTag<String> sideOutputTag1 = new TupleTag<String>("side1"){};
    TupleTag<String> sideOutputTag2 = new TupleTag<String>("side2"){};
    TupleTag<String> sideOutputTag3 = new TupleTag<String>("side3"){};
    TupleTag<String> sideOutputTagUnwritten = new TupleTag<String>("sideUnwritten"){};

    PCollectionTuple outputs = p
        .apply(Create.of(Arrays.asList(3, -42, 666))).setName("MyInput")
        .apply(ParDo
               .named("MyParDo")
               .of(new TestDoFn(
                   Arrays.<PCollectionView<Integer>>asList(),
                   Arrays.asList(sideOutputTag1, sideOutputTag2, sideOutputTag3)))
               .withOutputTags(
                   mainOutputTag,
                   TupleTagList.of(sideOutputTag3).and(sideOutputTag1)
                   .and(sideOutputTagUnwritten).and(sideOutputTag2)));

    assertEquals("MyParDo.main", outputs.get(mainOutputTag).getName());
    assertEquals("MyParDo.side1", outputs.get(sideOutputTag1).getName());
    assertEquals("MyParDo.side2", outputs.get(sideOutputTag2).getName());
    assertEquals("MyParDo.side3", outputs.get(sideOutputTag3).getName());
    assertEquals("MyParDo.sideUnwritten",
                 outputs.get(sideOutputTagUnwritten).getName());
  }

  @Test
  @Category(RunnableOnService.class)
  public void testParDoInCustomTransform() {
    Pipeline pipeline = TestPipeline.create();

    List<Integer> inputs = Arrays.asList(3, -42, 666);

    PCollection<String> output = pipeline
        .apply(Create.of(inputs))
        .apply("CustomTransform", new PTransform<PCollection<Integer>, PCollection<String>>() {
            @Override
            public PCollection<String> apply(PCollection<Integer> input) {
              return input.apply(ParDo.of(new TestDoFn()));
            }
          });

    // Test that Coder inference of the result works through
    // user-defined PTransforms.
    DataflowAssert.that(output)
        .satisfies(ParDoTest.HasExpectedOutput.forInput(inputs));

    pipeline.run();
  }

  @Test
  public void testMultiOutputChaining() {
    Pipeline pipeline = TestPipeline.create();

    PCollectionTuple filters = pipeline
        .apply(Create.of(Arrays.asList(3, 4, 5, 6)))
        .apply(new MultiFilter());
    PCollection<Integer> by2 = filters.get(MultiFilter.BY2);
    PCollection<Integer> by3 = filters.get(MultiFilter.BY3);

    // Apply additional filters to each operation.
    PCollection<Integer> by2then3 = by2
        .apply("Filter3sAgain", ParDo.of(new MultiFilter.FilterFn(3)));
    PCollection<Integer> by3then2 = by3
        .apply("Filter2sAgain", ParDo.of(new MultiFilter.FilterFn(2)));

    DataflowAssert.that(by2then3).containsInAnyOrder(6);
    DataflowAssert.that(by3then2).containsInAnyOrder(6);
    pipeline.run();
  }

  @Test
  public void testJsonEscaping() {
    // Declare an arbitrary function and make sure we can serialize it
    DoFn<Integer, Integer> doFn = new DoFn<Integer, Integer>() {
      @Override
      public void processElement(ProcessContext c) {
        c.output(c.element() + 1);
      }
    };

    byte[] serializedBytes = serializeToByteArray(doFn);
    String serializedJson = byteArrayToJsonString(serializedBytes);
    assertArrayEquals(
        serializedBytes, jsonStringToByteArray(serializedJson));
  }

  private static class TestDummy { }

  private static class TestDummyCoder extends AtomicCoder<TestDummy> {
    private TestDummyCoder() { }
    private static final TestDummyCoder INSTANCE = new TestDummyCoder();

    @JsonCreator
    public static TestDummyCoder of() {
      return INSTANCE;
    }

    @SuppressWarnings("unused") // used to create a CoderFactory
    public static List<Object> getInstanceComponents(TestDummy exampleValue) {
      return Collections.emptyList();
    }

    @Override
    public void encode(TestDummy value, OutputStream outStream, Context context)
        throws CoderException, IOException {
    }

    @Override
    public TestDummy decode(InputStream inStream, Context context)
        throws CoderException, IOException {
      return new TestDummy();
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(TestDummy value, Context context) {
      return true;
    }

    @Override
    public void registerByteSizeObserver(
        TestDummy value, ElementByteSizeObserver observer, Context context)
        throws Exception {
      observer.update(0L);
    }
  }

  private static class SideOutputDummyFn extends DoFn<Integer, Integer> {
    private TupleTag<TestDummy> sideTag;
    public SideOutputDummyFn(TupleTag<TestDummy> sideTag) {
      this.sideTag = sideTag;
    }
    @Override
    public void processElement(ProcessContext c) {
      c.output(1);
      c.sideOutput(sideTag, new TestDummy());
     }
  }

  private static class MainOutputDummyFn extends DoFn<Integer, TestDummy> {
    private TupleTag<Integer> sideTag;
    public MainOutputDummyFn(TupleTag<Integer> sideTag) {
      this.sideTag = sideTag;
    }
    @Override
    public void processElement(ProcessContext c) {
      c.output(new TestDummy());
      c.sideOutput(sideTag, 1);
     }
  }

  /** DataflowAssert "matcher" for expected output. */
  static class HasExpectedOutput
      implements SerializableFunction<Iterable<String>, Void>, Serializable {
    private final List<Integer> inputs;
    private final List<Integer> sideInputs;
    private final String sideOutput;
    private final boolean ordered;

    public static HasExpectedOutput forInput(List<Integer> inputs) {
      return new HasExpectedOutput(
          new ArrayList<Integer>(inputs),
          new ArrayList<Integer>(),
          "",
          false);
    }

    private HasExpectedOutput(List<Integer> inputs,
                              List<Integer> sideInputs,
                              String sideOutput,
                              boolean ordered) {
      this.inputs = inputs;
      this.sideInputs = sideInputs;
      this.sideOutput = sideOutput;
      this.ordered = ordered;
    }

    public HasExpectedOutput andSideInputs(Integer... sideInputValues) {
      List<Integer> sideInputs = new ArrayList<>();
      for (Integer sideInputValue : sideInputValues) {
        sideInputs.add(sideInputValue);
      }
      return new HasExpectedOutput(inputs, sideInputs, sideOutput, ordered);
    }

    public HasExpectedOutput fromSideOutput(TupleTag<String> sideOutputTag) {
      return fromSideOutput(sideOutputTag.getId());
    }
    public HasExpectedOutput fromSideOutput(String sideOutput) {
      return new HasExpectedOutput(inputs, sideInputs, sideOutput, ordered);
    }

    public HasExpectedOutput inOrder() {
      return new HasExpectedOutput(inputs, sideInputs, sideOutput, true);
    }

    @Override
    public Void apply(Iterable<String> outputs) {
      List<String> starteds = new ArrayList<>();
      List<String> processeds = new ArrayList<>();
      List<String> finisheds = new ArrayList<>();
      for (String output : outputs) {
        if (output.contains("started")) {
          starteds.add(output);
        } else if (output.contains("finished")) {
          finisheds.add(output);
        } else {
          processeds.add(output);
        }
      }

      String sideInputsSuffix;
      if (sideInputs.isEmpty()) {
        sideInputsSuffix = "";
      } else {
        sideInputsSuffix = ": " + sideInputs;
      }

      String sideOutputPrefix;
      if (sideOutput.isEmpty()) {
        sideOutputPrefix = "";
      } else {
        sideOutputPrefix = sideOutput + ": ";
      }

      List<String> expectedProcesseds = new ArrayList<>();
      for (Integer input : inputs) {
        expectedProcesseds.add(
            sideOutputPrefix + "processing: " + input + sideInputsSuffix);
      }
      String[] expectedProcessedsArray =
          expectedProcesseds.toArray(new String[expectedProcesseds.size()]);
      if (!ordered || expectedProcesseds.isEmpty()) {
        assertThat(processeds, containsInAnyOrder(expectedProcessedsArray));
      } else {
        assertThat(processeds, contains(expectedProcessedsArray));
      }

      assertEquals(starteds.size(), finisheds.size());
      for (String started : starteds) {
        assertEquals(sideOutputPrefix + "started", started);
      }
      for (String finished : finisheds) {
        assertEquals(sideOutputPrefix + "finished", finished);
      }

      return null;
    }
  }

  @Test
  public void testSideOutputUnknownCoder() throws Exception {
    Pipeline pipeline = TestPipeline.create();
    PCollection<Integer> input = pipeline
        .apply(Create.of(Arrays.asList(1, 2, 3)));

    final TupleTag<Integer> mainOutputTag = new TupleTag<Integer>("main");
    final TupleTag<TestDummy> sideOutputTag = new TupleTag<TestDummy>("unknownSide");
    input.apply(ParDo.of(new SideOutputDummyFn(sideOutputTag))
        .withOutputTags(mainOutputTag, TupleTagList.of(sideOutputTag)));

    thrown.expect(PipelineExecutionException.class);
    thrown.expectMessage("Unable to return a default Coder");
    pipeline.run();
  }

  @Test
  public void testSideOutputUnregisteredExplicitCoder() throws Exception {
    Pipeline pipeline = TestPipeline.create();
    PCollection<Integer> input = pipeline
        .apply(Create.of(Arrays.asList(1, 2, 3)));

    final TupleTag<Integer> mainOutputTag = new TupleTag<Integer>("main");
    final TupleTag<TestDummy> sideOutputTag = new TupleTag<TestDummy>("unregisteredSide");
    PCollectionTuple outputTuple = input.apply(ParDo.of(new SideOutputDummyFn(sideOutputTag))
        .withOutputTags(mainOutputTag, TupleTagList.of(sideOutputTag)));

    outputTuple.get(sideOutputTag).setCoder(new TestDummyCoder());

    outputTuple.get(sideOutputTag).apply(View.<TestDummy>asSingleton());

    assertEquals(new TestDummyCoder(), outputTuple.get(sideOutputTag).getCoder());
    outputTuple.get(sideOutputTag).finishSpecifyingOutput(); // Check for crashes
    assertEquals(new TestDummyCoder(),
        outputTuple.get(sideOutputTag).getCoder()); // Check for corruption
    pipeline.run();
  }

  @Test
  public void testMainOutputUnregisteredExplicitCoder() {
    Pipeline pipeline = TestPipeline.create();
    PCollection<Integer> input = pipeline
        .apply(Create.of(Arrays.asList(1, 2, 3)));

    final TupleTag<TestDummy> mainOutputTag = new TupleTag<TestDummy>("unregisteredMain");
    final TupleTag<Integer> sideOutputTag = new TupleTag<Integer>("side") {};
    PCollectionTuple outputTuple = input.apply(ParDo.of(new MainOutputDummyFn(sideOutputTag))
        .withOutputTags(mainOutputTag, TupleTagList.of(sideOutputTag)));

    outputTuple.get(mainOutputTag).setCoder(new TestDummyCoder());

    pipeline.run();
  }

  @Test
  public void testMainOutputApplySideOutputNoCoder() {
    // Regression test: applying a transform to the main output
    // should not cause a crash based on lack of a coder for the
    // side output.

    Pipeline pipeline = TestPipeline.create();
    final TupleTag<TestDummy> mainOutputTag = new TupleTag<TestDummy>("main");
    final TupleTag<TestDummy> sideOutputTag = new TupleTag<TestDummy>("side");
    PCollectionTuple tuple = pipeline
        .apply(Create.of(new TestDummy())
            .withCoder(TestDummyCoder.of()))
        .apply(ParDo
            .withOutputTags(mainOutputTag, TupleTagList.of(sideOutputTag))
            .of(
                new DoFn<TestDummy, TestDummy>() {
                  @Override public void processElement(ProcessContext context) {
                    TestDummy element = context.element();
                    context.output(element);
                    context.sideOutput(sideOutputTag, element);
                  }
                })
    );

    // Before fix, tuple.get(mainOutputTag).apply(...) would indirectly trigger
    // tuple.get(sideOutputTag).finishSpecifyingOutput(), which would crash
    // on a missing coder.
    tuple.get(mainOutputTag)
        .setCoder(TestDummyCoder.of())
        .apply("Output1", ParDo.of(new DoFn<TestDummy, Integer>() {
          @Override public void processElement(ProcessContext context) {
            context.output(1);
          }
        }));

    tuple.get(sideOutputTag).setCoder(TestDummyCoder.of());

    pipeline.run();
  }

  @Test
  public void testParDoOutputWithTimestamp() {
    Pipeline pipeline = TestPipeline.create();

    PCollection<Integer> input =
        pipeline.apply(Create.of(Arrays.asList(3, 42, 6)));

    PCollection<String> output =
        input
        .apply(ParDo.of(new TestOutputTimestampDoFn()))
        .apply(ParDo.of(new TestShiftTimestampDoFn(Duration.ZERO, Duration.ZERO)))
        .apply(ParDo.of(new TestFormatTimestampDoFn()));

    DataflowAssert.that(output).containsInAnyOrder(
                   "processing: 3, timestamp: 3",
                   "processing: 42, timestamp: 42",
                   "processing: 6, timestamp: 6");

    pipeline.run();
  }

  @Test
  public void testParDoSideOutputWithTimestamp() {
    Pipeline pipeline = TestPipeline.create();

    PCollection<Integer> input =
        pipeline.apply(Create.of(Arrays.asList(3, 42, 6)));

    final TupleTag<Integer> mainOutputTag = new TupleTag<Integer>("main"){};
    final TupleTag<Integer> sideOutputTag = new TupleTag<Integer>("side"){};

    PCollection<String> output =
        input
        .apply(ParDo.withOutputTags(mainOutputTag, TupleTagList.of(sideOutputTag)).of(
            new DoFn<Integer, Integer>() {
              @Override
              public void processElement(ProcessContext c) {
                c.sideOutputWithTimestamp(
                    sideOutputTag, c.element(), new Instant(c.element().longValue()));
              }
            })).get(sideOutputTag)
        .apply(ParDo.of(new TestShiftTimestampDoFn(Duration.ZERO, Duration.ZERO)))
        .apply(ParDo.of(new TestFormatTimestampDoFn()));

    DataflowAssert.that(output).containsInAnyOrder(
                   "processing: 3, timestamp: 3",
                   "processing: 42, timestamp: 42",
                   "processing: 6, timestamp: 6");

    pipeline.run();
  }

  @Test
  public void testParDoShiftTimestamp() {
    Pipeline pipeline = TestPipeline.create();

    PCollection<Integer> input =
        pipeline.apply(Create.of(Arrays.asList(3, 42, 6)));

    PCollection<String> output =
        input
        .apply(ParDo.of(new TestOutputTimestampDoFn()))
        .apply(ParDo.of(new TestShiftTimestampDoFn(Duration.millis(1000),
                                                   Duration.millis(-1000))))
        .apply(ParDo.of(new TestFormatTimestampDoFn()));

    DataflowAssert.that(output).containsInAnyOrder(
                   "processing: 3, timestamp: -997",
                   "processing: 42, timestamp: -958",
                   "processing: 6, timestamp: -994");

    pipeline.run();
  }

  @Test
  public void testParDoShiftTimestampInvalid() {
    Pipeline pipeline = TestPipeline.create();

    pipeline.apply(Create.of(Arrays.asList(3, 42, 6)))
        .apply(ParDo.of(new TestOutputTimestampDoFn()))
        .apply(ParDo.of(new TestShiftTimestampDoFn(Duration.millis(1000), // allowed skew = 1 second
                                                   Duration.millis(-1001))))
        .apply(ParDo.of(new TestFormatTimestampDoFn()));

    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Cannot output with timestamp");
    thrown.expectMessage(
        "Output timestamps must be no earlier than the timestamp of the current input");
    thrown.expectMessage("minus the allowed skew (1 second).");
    pipeline.run();
  }

  @Test
  public void testParDoShiftTimestampInvalidZeroAllowed() {
    Pipeline pipeline = TestPipeline.create();

    pipeline.apply(Create.of(Arrays.asList(3, 42, 6)))
        .apply(ParDo.of(new TestOutputTimestampDoFn()))
        .apply(ParDo.of(new TestShiftTimestampDoFn(Duration.ZERO,
                                                   Duration.millis(-1001))))
        .apply(ParDo.of(new TestFormatTimestampDoFn()));

    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Cannot output with timestamp");
    thrown.expectMessage(
        "Output timestamps must be no earlier than the timestamp of the current input");
    thrown.expectMessage("minus the allowed skew (0 milliseconds).");
    pipeline.run();
  }

  private static class Checker implements SerializableFunction<Iterable<String>, Void> {
    @Override
    public Void apply(Iterable<String> input) {
      boolean foundStart = false;
      boolean foundElement = false;
      boolean foundFinish = false;
      for (String str : input) {
        if (str.equals("elem:1:1")) {
          if (foundElement) {
            throw new AssertionError("Received duplicate element");
          }
          foundElement = true;
        } else if (str.equals("start:2:2")) {
          foundStart = true;
        } else if (str.equals("finish:3:3")) {
          foundFinish = true;
        } else {
          throw new AssertionError("Got unexpected value: " + str);
        }
      }
      if (!foundStart) {
        throw new AssertionError("Missing \"start:2:2\"");
      }
      if (!foundElement) {
        throw new AssertionError("Missing \"elem:1:1\"");
      }
      if (!foundFinish) {
        throw new AssertionError("Missing \"finish:3:3\"");
      }

      return null;
    }
  }

  @Test
  @Category(RunnableOnService.class)
  public void testWindowingInStartAndFinishBundle() {
    Pipeline pipeline = TestPipeline.create();

    PCollection<String> output = pipeline
        .apply(Create.timestamped(TimestampedValue.of("elem", new Instant(1))))
        .apply(Window.<String>into(FixedWindows.of(Duration.millis(1))))
        .apply(ParDo.of(new DoFn<String, String>() {
                  @Override
                  public void startBundle(Context c) {
                    c.outputWithTimestamp("start", new Instant(2));
                    System.out.println("Start: 2");
                  }

                  @Override
                  public void processElement(ProcessContext c) {
                    c.output(c.element());
                    System.out.println("Process: " + c.element() + ":" + c.timestamp().getMillis());
                  }

                  @Override
                  public void finishBundle(Context c) {
                    c.outputWithTimestamp("finish", new Instant(3));
                    System.out.println("Finish: 3");
                  }
                }))
        .apply(ParDo.of(new PrintingDoFn()));

    DataflowAssert.that(output).satisfies(new Checker());

    pipeline.run();
  }

  @Test
  public void testWindowingInStartBundleException() {
    Pipeline pipeline = TestPipeline.create();

    pipeline
        .apply(Create.timestamped(TimestampedValue.of("elem", new Instant(1))))
        .apply(Window.<String>into(FixedWindows.of(Duration.millis(1))))
        .apply(ParDo.of(new DoFn<String, String>() {
                  @Override
                  public void startBundle(Context c) {
                    c.output("start");
                  }

                  @Override
                  public void processElement(ProcessContext c) {
                    c.output(c.element());
                  }
                }));

    thrown.expectMessage("WindowFn attempted to access input timestamp when none was available");
    pipeline.run();
  }

  /**
   * Tests that a {@link DoFn} that mutates an output with a good equals() fails in the
   * {@link DirectPipelineRunner}.
   */
  @Test
  public void testMutatingOutputThenOutputDoFnError() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    pipeline
        .apply(Create.of(42))
        .apply(ParDo.of(new DoFn<Integer, List<Integer>>() {
          @Override public void processElement(ProcessContext c) {
            List<Integer> outputList = Arrays.asList(1, 2, 3, 4);
            c.output(outputList);
            outputList.set(0, 37);
            c.output(outputList);
          }
        }));

    thrown.expect(PipelineExecutionException.class);
    thrown.expectCause(isA(IllegalMutationException.class));
    thrown.expectMessage("output");
    thrown.expectMessage("must not be mutated");
    pipeline.run();
  }

  /**
   * Tests that a {@link DoFn} that mutates an output with a good equals() fails in the
   * {@link DirectPipelineRunner}.
   */
  @Test
  public void testMutatingOutputThenTerminateDoFnError() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    pipeline
        .apply(Create.of(42))
        .apply(ParDo.of(new DoFn<Integer, List<Integer>>() {
          @Override public void processElement(ProcessContext c) {
            List<Integer> outputList = Arrays.asList(1, 2, 3, 4);
            c.output(outputList);
            outputList.set(0, 37);
          }
        }));

    thrown.expect(IllegalMutationException.class);
    thrown.expectMessage("output");
    thrown.expectMessage("must not be mutated");
    pipeline.run();
  }

  /**
   * Tests that a {@link DoFn} that mutates an output with a bad equals() still fails
   * in the {@link DirectPipelineRunner}.
   */
  @Test
  public void testMutatingOutputCoderDoFnError() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    pipeline
        .apply(Create.of(42))
        .apply(ParDo.of(new DoFn<Integer, byte[]>() {
          @Override public void processElement(ProcessContext c) {
            byte[] outputArray = new byte[]{0x1, 0x2, 0x3};
            c.output(outputArray);
            outputArray[0] = 0xa;
            c.output(outputArray);
          }
        }));

    thrown.expect(PipelineExecutionException.class);
    thrown.expectCause(isA(IllegalMutationException.class));
    thrown.expectMessage("output");
    thrown.expectMessage("must not be mutated");
    pipeline.run();
  }

  /**
   * Tests that a {@link DoFn} that mutates its input with a good equals() fails in the
   * {@link DirectPipelineRunner}.
   */
  @Test
  public void testMutatingInputDoFnError() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    pipeline
        .apply(Create.of(Arrays.asList(1, 2, 3), Arrays.asList(4, 5, 6))
            .withCoder(ListCoder.of(VarIntCoder.of())))
        .apply(ParDo.of(new DoFn<List<Integer>, Integer>() {
          @Override public void processElement(ProcessContext c) {
            List<Integer> inputList = c.element();
            inputList.set(0, 37);
            c.output(12);
          }
        }));

    thrown.expect(IllegalMutationException.class);
    thrown.expectMessage("input");
    thrown.expectMessage("must not be mutated");
    pipeline.run();
  }

  /**
   * Tests that a {@link DoFn} that mutates an input with a bad equals() still fails
   * in the {@link DirectPipelineRunner}.
   */
  @Test
  public void testMutatingInputCoderDoFnError() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    pipeline
        .apply(Create.of(new byte[]{0x1, 0x2, 0x3}, new byte[]{0x4, 0x5, 0x6}))
        .apply(ParDo.of(new DoFn<byte[], Integer>() {
          @Override public void processElement(ProcessContext c) {
            byte[] inputArray = c.element();
            inputArray[0] = 0xa;
            c.output(13);
          }
        }));

    thrown.expect(IllegalMutationException.class);
    thrown.expectMessage("input");
    thrown.expectMessage("must not be mutated");
    pipeline.run();
  }

  @Test
  public void testIncludesDoFnDisplayData() {
    Bound<String, String> parDo =
        ParDo.of(
            new DoFn<String, String>() {
              @Override
              public void processElement(ProcessContext c) {}

              @Override
              public void populateDisplayData(Builder builder) {
                builder.add("foo", "bar");
              }
            });

    DisplayData displayData = DisplayData.from(parDo);
    assertThat(displayData, hasDisplayItem(hasKey("foo")));
  }
}
