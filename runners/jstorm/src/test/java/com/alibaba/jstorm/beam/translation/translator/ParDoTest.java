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
package com.alibaba.jstorm.beam.translation.translator;

import com.alibaba.jstorm.beam.StormPipelineOptions;

import com.alibaba.jstorm.beam.TestJStormRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link ParDo} with {@link com.alibaba.jstorm.beam.StormRunner}.
 */
@RunWith(JUnit4.class)
public class ParDoTest implements Serializable {

    @Test
    public void testParDo() throws IOException {
        StormPipelineOptions options = PipelineOptionsFactory.as(StormPipelineOptions.class);
        options.setRunner(TestJStormRunner.class);
        options.setLocalMode(true);

        Pipeline pipeline = Pipeline.create(options);

        List<Integer> inputs = Arrays.asList(3, -42, 666);

        PCollection<String> output = pipeline
                .apply(Create.of(inputs))
                .apply(ParDo.of(new TestDoFn()));

        PAssert.that(output)
                .satisfies(ParDoTest.HasExpectedOutput.forInput(inputs));

        pipeline.run();
    }

    @Test
    public void testParDoWithSideInputs() throws IOException {
        StormPipelineOptions options = PipelineOptionsFactory.as(StormPipelineOptions.class);
        options.setRunner(TestJStormRunner.class);
        options.setLocalMode(true);

        Pipeline pipeline = Pipeline.create(options);

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
                .apply(ParDo.of(new TestDoFn(
                                Arrays.asList(sideInput1, sideInput2),
                                Arrays.<TupleTag<String>>asList()))
                        .withSideInputs(sideInput1, sideInputUnread, sideInput2));

        PAssert.that(output)
                .satisfies(ParDoTest.HasExpectedOutput
                        .forInput(inputs)
                        .andSideInputs(11, 222));

        pipeline.run();
    }

    @Test
    public void testParDoWithTaggedOutput() {
        List<Integer> inputs = Arrays.asList(3, -42, 666);

        TupleTag<String> mainOutputTag = new TupleTag<String>("main"){};
        TupleTag<String> additionalOutputTag1 = new TupleTag<String>("additional1"){};
        TupleTag<String> additionalOutputTag2 = new TupleTag<String>("additional2"){};
        TupleTag<String> additionalOutputTag3 = new TupleTag<String>("additional3"){};
        TupleTag<String> additionalOutputTagUnwritten = new TupleTag<String>("unwrittenOutput"){};

        StormPipelineOptions options = PipelineOptionsFactory.as(StormPipelineOptions.class);
        options.setRunner(TestJStormRunner.class);
        options.setLocalMode(true);

        Pipeline pipeline = Pipeline.create(options);

        PCollectionTuple outputs = pipeline
            .apply(Create.of(inputs))
            .apply(ParDo
                .of(new TestDoFn(
                    Arrays.<PCollectionView<Integer>>asList(),
                    Arrays.asList(additionalOutputTag1, additionalOutputTag2, additionalOutputTag3)))
                .withOutputTags(
                    mainOutputTag,
                    TupleTagList.of(additionalOutputTag3)
                        .and(additionalOutputTag1)
                        .and(additionalOutputTagUnwritten)
                        .and(additionalOutputTag2)));

        PAssert.that(outputs.get(mainOutputTag))
            .satisfies(ParDoTest.HasExpectedOutput.forInput(inputs));

        PAssert.that(outputs.get(additionalOutputTag1))
            .satisfies(ParDoTest.HasExpectedOutput.forInput(inputs)
                .fromOutput(additionalOutputTag1));
        PAssert.that(outputs.get(additionalOutputTag2))
            .satisfies(ParDoTest.HasExpectedOutput.forInput(inputs)
                .fromOutput(additionalOutputTag2));
        PAssert.that(outputs.get(additionalOutputTag3))
            .satisfies(ParDoTest.HasExpectedOutput.forInput(inputs)
                .fromOutput(additionalOutputTag3));
        PAssert.that(outputs.get(additionalOutputTagUnwritten)).empty();

        pipeline.run();
    }

    @Test
    public void testNoWindowFnDoesNotReassignWindows() {
        StormPipelineOptions options = PipelineOptionsFactory.as(StormPipelineOptions.class);
        options.setRunner(TestJStormRunner.class);
        options.setLocalMode(true);

        Pipeline pipeline = Pipeline.create(options);

        final PCollection<Long> initialWindows =
                pipeline
                    .apply(GenerateSequence.from(0).to(10))
                    .apply("AssignWindows", Window.into(new WindowOddEvenBuckets()));

        // Sanity check the window assignment to demonstrate the baseline
        PAssert.that(initialWindows)
                .inWindow(WindowOddEvenBuckets.EVEN_WINDOW)
                .containsInAnyOrder(0L, 2L, 4L, 6L, 8L);
        PAssert.that(initialWindows)
                .inWindow(WindowOddEvenBuckets.ODD_WINDOW)
                .containsInAnyOrder(1L, 3L, 5L, 7L, 9L);

        PCollection<Boolean> upOne =
                initialWindows.apply(
                        "ModifyTypes",
                        MapElements.<Long, Boolean>via(
                                new SimpleFunction<Long, Boolean>() {
                                    @Override
                                    public Boolean apply(Long input) {
                                        return input % 2 == 0;
                                    }
                                }));
        PAssert.that(upOne)
                .inWindow(WindowOddEvenBuckets.EVEN_WINDOW)
                .containsInAnyOrder(true, true, true, true, true);
        PAssert.that(upOne)
                .inWindow(WindowOddEvenBuckets.ODD_WINDOW)
                .containsInAnyOrder(false, false, false, false, false);

        // The elements should be in the same windows, even though they would not be assigned to the
        // same windows with the updated timestamps. If we try to apply the original WindowFn, the type
        // will not be appropriate and the runner should crash, as a Boolean cannot be converted into
        // a long.
        PCollection<Boolean> updatedTrigger =
                upOne.apply(
                        "UpdateWindowingStrategy",
                        Window.<Boolean>configure().triggering(Never.ever())
                                .withAllowedLateness(Duration.ZERO)
                                .accumulatingFiredPanes());
        pipeline.run();
    }


    private static class WindowOddEvenBuckets extends NonMergingWindowFn<Long, IntervalWindow> {
        private static final IntervalWindow EVEN_WINDOW =
                new IntervalWindow(
                        BoundedWindow.TIMESTAMP_MIN_VALUE, GlobalWindow.INSTANCE.maxTimestamp());
        private static final IntervalWindow ODD_WINDOW =
                new IntervalWindow(
                        BoundedWindow.TIMESTAMP_MIN_VALUE, GlobalWindow.INSTANCE.maxTimestamp().minus(1));

        @Override
        public Collection<IntervalWindow> assignWindows(AssignContext c) throws Exception {
            if (c.element() % 2 == 0) {
                return Collections.singleton(EVEN_WINDOW);
            }
            return Collections.singleton(ODD_WINDOW);
        }

        @Override
        public boolean isCompatible(WindowFn<?, ?> other) {
            return other instanceof WindowOddEvenBuckets;
        }

        @Override
        public Coder<IntervalWindow> windowCoder() {
            return new IntervalWindow.IntervalWindowCoder();
        }

        @Override
        public WindowMappingFn<IntervalWindow> getDefaultWindowMappingFn() {
            throw new UnsupportedOperationException(
                    String.format("Can't use %s for side inputs", getClass().getSimpleName()));
        }
    }


    static class TestDoFn extends DoFn<Integer, String> {
        enum State {NOT_SET_UP, UNSTARTED, STARTED, PROCESSING, FINISHED}

        State state = State.NOT_SET_UP;

        final List<PCollectionView<Integer>> sideInputViews = new ArrayList<>();
        final List<TupleTag<String>> additionalOutputTupleTags = new ArrayList<>();

        public TestDoFn() {
        }

        public TestDoFn(List<PCollectionView<Integer>> sideInputViews,
                        List<TupleTag<String>> additionalOutputTupleTags) {
            this.sideInputViews.addAll(sideInputViews);
            this.additionalOutputTupleTags.addAll(additionalOutputTupleTags);
        }

        @Setup
        public void prepare() {
            assertEquals(State.NOT_SET_UP, state);
            state = State.UNSTARTED;
        }

        @StartBundle
        public void startBundle() {
            assertThat(state,
                anyOf(equalTo(State.UNSTARTED), equalTo(State.FINISHED)));

            state = State.STARTED;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            System.out.println("Recv elem: " + c.element());
            assertThat(state,
                anyOf(equalTo(State.STARTED), equalTo(State.PROCESSING)));
            state = State.PROCESSING;
            outputToAllWithSideInputs(c, "processing: " + c.element());
        }

        @FinishBundle
        public void finishBundle(FinishBundleContext c) {
            assertThat(state,
                anyOf(equalTo(State.STARTED), equalTo(State.PROCESSING)));
            state = State.FINISHED;
            c.output("finished", BoundedWindow.TIMESTAMP_MIN_VALUE, GlobalWindow.INSTANCE);
            for (TupleTag<String> additionalOutputTupleTag : additionalOutputTupleTags) {
                c.output(
                    additionalOutputTupleTag,
                    additionalOutputTupleTag.getId() + ": " + "finished",
                    BoundedWindow.TIMESTAMP_MIN_VALUE,
                    GlobalWindow.INSTANCE);
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
            for (TupleTag<String> additionalOutputTupleTag : additionalOutputTupleTags) {
                c.output(additionalOutputTupleTag,
                    additionalOutputTupleTag.getId() + ": " + value);
            }
        }
    }

    /** PAssert "matcher" for expected output. */
    static class HasExpectedOutput
        implements SerializableFunction<Iterable<String>, Void>, Serializable {
        private final List<Integer> inputs;
        private final List<Integer> sideInputs;
        private final String additionalOutput;
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
                                  String additionalOutput,
                                  boolean ordered) {
            this.inputs = inputs;
            this.sideInputs = sideInputs;
            this.additionalOutput = additionalOutput;
            this.ordered = ordered;
        }

        public HasExpectedOutput andSideInputs(Integer... sideInputValues) {
            List<Integer> sideInputs = new ArrayList<>();
            for (Integer sideInputValue : sideInputValues) {
                sideInputs.add(sideInputValue);
            }
            return new HasExpectedOutput(inputs, sideInputs, additionalOutput, ordered);
        }

        public HasExpectedOutput fromOutput(TupleTag<String> outputTag) {
            return fromOutput(outputTag.getId());
        }
        public HasExpectedOutput fromOutput(String outputId) {
            return new HasExpectedOutput(inputs, sideInputs, outputId, ordered);
        }

        public HasExpectedOutput inOrder() {
            return new HasExpectedOutput(inputs, sideInputs, additionalOutput, true);
        }

        @Override
        public Void apply(Iterable<String> outputs) {
            List<String> processeds = new ArrayList<>();
            List<String> finisheds = new ArrayList<>();
            for (String output : outputs) {
                if (output.contains("finished")) {
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

            String additionalOutputPrefix;
            if (additionalOutput.isEmpty()) {
                additionalOutputPrefix = "";
            } else {
                additionalOutputPrefix = additionalOutput + ": ";
            }

            List<String> expectedProcesseds = new ArrayList<>();
            for (Integer input : inputs) {
                expectedProcesseds.add(
                    additionalOutputPrefix + "processing: " + input + sideInputsSuffix);
            }
            String[] expectedProcessedsArray =
                expectedProcesseds.toArray(new String[expectedProcesseds.size()]);
            if (!ordered || expectedProcesseds.isEmpty()) {
                assertThat(processeds, containsInAnyOrder(expectedProcessedsArray));
            } else {
                assertThat(processeds, contains(expectedProcessedsArray));
            }

            for (String finished : finisheds) {
                assertEquals(additionalOutputPrefix + "finished", finished);
            }

            return null;
        }
    }
}
