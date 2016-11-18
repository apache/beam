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
package org.apache.beam.sdk.microbenchmarks.transforms;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnAdapters;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker.ArgumentProvider;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker.FakeArgumentProvider;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowingInternals;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Benchmarks for {@link OldDoFn} and {@link DoFn} invocations, specifically for measuring the
 * overhead of {@link DoFnInvokers}.
 */
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 5)
public class DoFnInvokersBenchmark {

  private static final String ELEMENT = "some string to use for testing";

  private OldDoFn<String, String> oldDoFn = new UpperCaseOldDoFn();
  private DoFn<String, String> doFn = new UpperCaseDoFn();

  private StubOldDoFnProcessContext stubOldDoFnContext =
      new StubOldDoFnProcessContext(oldDoFn, ELEMENT);
  private StubDoFnProcessContext stubDoFnContext = new StubDoFnProcessContext(doFn, ELEMENT);
  private ArgumentProvider<String, String> argumentProvider =
      new FakeArgumentProvider<>();

  private OldDoFn<String, String> adaptedDoFnWithContext;

  private DoFnInvoker<String, String> invoker;

  @Setup
  public void setUp() {
    adaptedDoFnWithContext = DoFnAdapters.toOldDoFn(doFn);
    invoker = DoFnInvokers.invokerFor(doFn);
  }

  @Benchmark
  public String invokeOldDoFn() throws Exception {
    oldDoFn.processElement(stubOldDoFnContext);
    return stubDoFnContext.output;
  }

  @Benchmark
  public String invokeDoFnWithContextViaAdaptor() throws Exception {
    adaptedDoFnWithContext.processElement(stubOldDoFnContext);
    return stubOldDoFnContext.output;
  }

  @Benchmark
  public String invokeDoFnWithContext() throws Exception {
    invoker.invokeProcessElement(argumentProvider);
    return stubDoFnContext.output;
  }

  private static class UpperCaseOldDoFn extends OldDoFn<String, String> {

    @Override
    public void processElement(ProcessContext c) throws Exception {
      c.output(c.element().toUpperCase());
    }
  }

  private static class UpperCaseDoFn extends DoFn<String, String> {

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      c.output(c.element().toUpperCase());
    }
  }

  private static class StubOldDoFnProcessContext extends OldDoFn<String, String>.ProcessContext {

    private final String element;
    private String output;

    public StubOldDoFnProcessContext(OldDoFn<String, String> fn, String element) {
      fn.super();
      this.element = element;
    }

    @Override
    public String element() {
      return element;
    }

    @Override
    public <T> T sideInput(PCollectionView<T> view) {
      return null;
    }

    @Override
    public Instant timestamp() {
      return null;
    }

    @Override
    public BoundedWindow window() {
      return null;
    }

    @Override
    public PaneInfo pane() {
      return null;
    }

    @Override
    public WindowingInternals<String, String> windowingInternals() {
      return null;
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return null;
    }

    @Override
    public void output(String output) {
      this.output = output;
    }

    @Override
    public void outputWithTimestamp(String output, Instant timestamp) {
      output(output);
    }

    @Override
    public <T> void sideOutput(TupleTag<T> tag, T output) {}

    @Override
    public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {}

    @Override
    protected <AggInputT, AggOutputT> Aggregator<AggInputT, AggOutputT> createAggregatorInternal(
        String name, CombineFn<AggInputT, ?, AggOutputT> combiner) {
      return null;
    }
  }

  private static class StubDoFnProcessContext extends DoFn<String, String>.ProcessContext {
    private final String element;
    private String output;

    public StubDoFnProcessContext(DoFn<String, String> fn, String element) {
      fn.super();
      this.element = element;
    }

    @Override
    public String element() {
      return element;
    }

    @Override
    public <T> T sideInput(PCollectionView<T> view) {
      return null;
    }

    @Override
    public Instant timestamp() {
      return null;
    }

    @Override
    public PaneInfo pane() {
      return null;
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return null;
    }

    @Override
    public void output(String output) {
      this.output = output;
    }

    @Override
    public void outputWithTimestamp(String output, Instant timestamp) {
      output(output);
    }

    @Override
    public <T> void sideOutput(TupleTag<T> tag, T output) {}

    @Override
    public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {}

    @Override
    protected <AggInputT, AggOutputT> Aggregator<AggInputT, AggOutputT> createAggregator(
        String name,
        CombineFn<AggInputT, ?, AggOutputT> combiner) {
      return null;
    }
  }
}
