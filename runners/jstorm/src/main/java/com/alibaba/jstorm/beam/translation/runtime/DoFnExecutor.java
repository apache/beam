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
package com.alibaba.jstorm.beam.translation.runtime;

import java.io.Serializable;
import java.util.*;

import avro.shaded.com.google.common.collect.Iterables;
import com.alibaba.jstorm.beam.translation.runtime.state.JStormStateInternals;
import com.alibaba.jstorm.beam.translation.runtime.timer.JStormTimerInternals;

import com.alibaba.jstorm.cache.IKvStoreManager;
import com.alibaba.jstorm.metric.MetricClient;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.runners.core.PushbackSideInputDoFnRunner;
import org.apache.beam.runners.core.SideInputHandler;
import org.apache.beam.runners.core.SimplePushbackSideInputDoFnRunner;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.beam.StormPipelineOptions;
import com.alibaba.jstorm.beam.translation.util.DefaultStepContext;
import com.alibaba.jstorm.beam.util.SerializedPipelineOptions;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class DoFnExecutor<InputT, OutputT> implements Executor {
    private static final long serialVersionUID = 5297603063991078668L;

    private static final Logger LOG = LoggerFactory.getLogger(DoFnExecutor.class);

    public class DoFnExecutorOutputManager implements OutputManager, Serializable {
        private static final long serialVersionUID = -661113364735206170L;

        @Override
        public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
            executorsBolt.processExecutorElem(tag, output);
        }
    }

    protected transient DoFnRunner<InputT, OutputT> runner = null;
    protected transient PushbackSideInputDoFnRunner<InputT, OutputT> pushbackRunner = null;

    private final String description;

    protected final TupleTag<OutputT> mainTupleTag;
    protected final List<TupleTag<?>> sideOutputTags;

    protected SerializedPipelineOptions serializedOptions;
    protected transient StormPipelineOptions pipelineOptions;

    protected DoFn<InputT, OutputT> doFn;
    private final Coder<WindowedValue<InputT>> inputCoder;
    protected DoFnInvoker<InputT, OutputT> doFnInvoker;
    protected DoFnExecutorOutputManager outputManager;
    protected WindowingStrategy<?, ?> windowingStrategy;
    private final TupleTag<InputT> mainInputTag;
    protected Collection<PCollectionView<?>> sideInputs;
    private SideInputHandler sideInputHandler;
    private final Map<TupleTag, PCollectionView<?>> sideInputTagToView;

    // Initialize during runtime
    protected ExecutorContext executorContext;
    protected ExecutorsBolt executorsBolt;
    protected TimerInternals timerInternals;
    private transient StateInternals pushbackStateInternals;
    private transient StateTag<BagState<WindowedValue<InputT>>> pushedBackTag;
    private transient StateTag<WatermarkHoldState> watermarkHoldTag;
    protected transient IKvStoreManager kvStoreManager;
    protected DefaultStepContext stepContext;

    public DoFnExecutor(
            String description,
            StormPipelineOptions pipelineOptions,
            DoFn<InputT, OutputT> doFn,
            Coder<WindowedValue<InputT>> inputCoder,
            WindowingStrategy<?, ?> windowingStrategy,
            TupleTag<InputT> mainInputTag,
            Collection<PCollectionView<?>> sideInputs,
            Map<TupleTag, PCollectionView<?>> sideInputTagToView,
            TupleTag<OutputT> mainTupleTag,
            List<TupleTag<?>> sideOutputTags) {
        this.description = checkNotNull(description, "description");
        this.serializedOptions = new SerializedPipelineOptions(pipelineOptions);
        this.doFn = doFn;
        this.inputCoder = inputCoder;
        this.outputManager = new DoFnExecutorOutputManager();
        this.windowingStrategy = windowingStrategy;
        this.mainInputTag = mainInputTag;
        this.sideInputs = sideInputs;
        this.mainTupleTag = mainTupleTag;
        this.sideOutputTags = sideOutputTags;
        this.sideInputTagToView = sideInputTagToView;
    }

    protected DoFnRunner<InputT, OutputT> getSimpleRunner() {
        return DoFnRunners.simpleRunner(
                this.pipelineOptions,
                this.doFn,
                this.sideInputHandler == null ? NullSideInputReader.empty() : sideInputHandler,
                this.outputManager,
                this.mainTupleTag,
                this.sideOutputTags,
                this.stepContext,
                this.windowingStrategy);
    }

    protected void initService(ExecutorContext context) {
        // TODO: what should be set for key in here?
        timerInternals = new JStormTimerInternals(null /* key */, this, context.getExecutorsBolt().timerService());
        kvStoreManager = context.getKvStoreManager();
        stepContext = new DefaultStepContext(timerInternals, new JStormStateInternals("state", kvStoreManager, executorsBolt.timerService()));
    }

    @Override
    public void init(ExecutorContext context) {
        this.executorContext = context;
        this.executorsBolt = context.getExecutorsBolt();
        this.pipelineOptions = this.serializedOptions.getPipelineOptions().as(StormPipelineOptions.class);

        initService(context);

        // Side inputs setup
        if (sideInputs == null || sideInputs.isEmpty()) {
            runner = getSimpleRunner();
        } else {
            pushedBackTag = StateTags.bag("pushed-back-values", inputCoder);
            watermarkHoldTag =
                    StateTags.watermarkStateInternal("hold", TimestampCombiner.EARLIEST);
            pushbackStateInternals = new JStormStateInternals(null, kvStoreManager, executorsBolt.timerService());
            sideInputHandler = new SideInputHandler(sideInputs, pushbackStateInternals);
            pushbackRunner = SimplePushbackSideInputDoFnRunner.create(getSimpleRunner(), sideInputs, sideInputHandler);
        }

        // Process user's setup
        doFnInvoker = DoFnInvokers.invokerFor(doFn);
        doFnInvoker.invokeSetup();
    }

    @Override
    public <T> void process(TupleTag<T> tag, WindowedValue<T> elem) {
        if (mainInputTag.equals(tag)) {
            processMainInput(elem);
        } else {
            processSideInput(tag, elem);
        }
    }

    private <T> void processMainInput(WindowedValue<T> elem) {
       if (sideInputs.isEmpty()) {
           runner.processElement((WindowedValue<InputT>) elem);
       } else {
           Iterable<WindowedValue<InputT>> justPushedBack =
               pushbackRunner.processElementInReadyWindows((WindowedValue<InputT>) elem);
           BagState<WindowedValue<InputT>> pushedBack =
                   pushbackStateInternals.state(StateNamespaces.global(), pushedBackTag);

           Instant min = BoundedWindow.TIMESTAMP_MAX_VALUE;
           for (WindowedValue<InputT> pushedBackValue : justPushedBack) {
               if (pushedBackValue.getTimestamp().isBefore(min)) {
                   min = pushedBackValue.getTimestamp();
               }
               min = earlier(min, pushedBackValue.getTimestamp());
               pushedBack.add(pushedBackValue);
           }
           pushbackStateInternals.state(StateNamespaces.global(), watermarkHoldTag).add(min);
       }
    }

    private void processSideInput(TupleTag tag, WindowedValue elem) {
        LOG.debug(String.format("side inputs: %s, %s.", tag, elem));

        PCollectionView<?> sideInputView = sideInputTagToView.get(tag);
        sideInputHandler.addSideInputValue(sideInputView, elem);

        BagState<WindowedValue<InputT>> pushedBack =
                pushbackStateInternals.state(StateNamespaces.global(), pushedBackTag);

        List<WindowedValue<InputT>> newPushedBack = new ArrayList<>();

        Iterable<WindowedValue<InputT>> pushedBackInputs = pushedBack.read();
        if (pushedBackInputs != null) {
            for (WindowedValue<InputT> input : pushedBackInputs) {

                Iterable<WindowedValue<InputT>> justPushedBack =
                        pushbackRunner.processElementInReadyWindows(input);
                Iterables.addAll(newPushedBack, justPushedBack);
            }
        }
        pushedBack.clear();

        Instant min = BoundedWindow.TIMESTAMP_MAX_VALUE;
        for (WindowedValue<InputT> pushedBackValue : newPushedBack) {
            min = earlier(min, pushedBackValue.getTimestamp());
            pushedBack.add(pushedBackValue);
        }

        WatermarkHoldState watermarkHold =
                pushbackStateInternals.state(StateNamespaces.global(), watermarkHoldTag);
        // TODO: clear-then-add is not thread-safe.
        watermarkHold.clear();
        watermarkHold.add(min);
    }

    public void onTimer(Object key, TimerInternals.TimerData timerData) {
        StateNamespace namespace = timerData.getNamespace();
        checkArgument(namespace instanceof StateNamespaces.WindowNamespace);
        BoundedWindow window = ((StateNamespaces.WindowNamespace) namespace).getWindow();
        runner.onTimer(timerData.getTimerId(), window, timerData.getTimestamp(), timerData.getDomain());
    }

    @Override
    public void cleanup() {
        doFnInvoker.invokeTeardown();
    }

    @Override
    public String toString() {
        return description;
    }

    private Instant earlier(Instant left, Instant right) {
        return left.isBefore(right) ? left : right;
    }

    public DoFnRunner<InputT, OutputT> getRunner() {
        return runner;
    }
}
