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

package org.apache.beam.runners.spark.translation;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import java.util.Iterator;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;


/**
 * Spark runner process context processes Spark partitions using Beam's {@link DoFnRunner}.
 */
class SparkProcessContext<FnInputT, FnOutputT, OutputT> {

  private final DoFn<FnInputT, FnOutputT> doFn;
  private final DoFnRunner<FnInputT, FnOutputT> doFnRunner;
  private final SparkOutputManager<OutputT> outputManager;
  private Iterator<TimerInternals.TimerData> timerDataIterator;

  SparkProcessContext(
      DoFn<FnInputT, FnOutputT> doFn,
      DoFnRunner<FnInputT, FnOutputT> doFnRunner,
      SparkOutputManager<OutputT> outputManager,
      Iterator<TimerInternals.TimerData> timerDataIterator) {

    this.doFn = doFn;
    this.doFnRunner = doFnRunner;
    this.outputManager = outputManager;
    this.timerDataIterator = timerDataIterator;
  }

  Iterable<OutputT> processPartition(
      Iterator<WindowedValue<FnInputT>> partition) throws Exception {

    // setup DoFn.
    DoFnInvokers.invokerFor(doFn).invokeSetup();

    // skip if partition is empty.
    if (!partition.hasNext()) {
      DoFnInvokers.invokerFor(doFn).invokeTeardown();
      return Lists.newArrayList();
    }

    // call startBundle() before beginning to process the partition.
    doFnRunner.startBundle();
    // process the partition; finishBundle() is called from within the output iterator.
    return this.getOutputIterable(partition, doFnRunner);
  }

  private void clearOutput() {
    outputManager.clear();
  }

  private Iterator<OutputT> getOutputIterator() {
    return outputManager.iterator();
  }

  private Iterable<OutputT> getOutputIterable(
      final Iterator<WindowedValue<FnInputT>> iter,
      final DoFnRunner<FnInputT, FnOutputT> doFnRunner) {

    return new Iterable<OutputT>() {
      @Override
      public Iterator<OutputT> iterator() {
        return new ProcCtxtIterator(iter, doFnRunner);
      }
    };
  }

  interface SparkOutputManager<T> extends OutputManager, Iterable<T> {

    void clear();

  }

  static class NoOpStepContext implements StepContext {

    @Override
    public StateInternals stateInternals() {
      return null;
    }

    @Override
    public TimerInternals timerInternals() {
      return null;
    }
  }

  private class ProcCtxtIterator extends AbstractIterator<OutputT> {

    private final Iterator<WindowedValue<FnInputT>> inputIterator;
    private final DoFnRunner<FnInputT, FnOutputT> doFnRunner;
    private Iterator<OutputT> outputIterator;
    private boolean calledFinish;

    ProcCtxtIterator(
        Iterator<WindowedValue<FnInputT>> iterator,
        DoFnRunner<FnInputT, FnOutputT> doFnRunner) {
      this.inputIterator = iterator;
      this.doFnRunner = doFnRunner;
      this.outputIterator = getOutputIterator();
    }

    @Override
    protected OutputT computeNext() {
      // Process each element from the (input) iterator, which produces, zero, one or more
      // output elements (of type V) in the output iterator. Note that the output
      // collection (and iterator) is reset between each call to processElement, so the
      // collection only holds the output values for each call to processElement, rather
      // than for the whole partition (which would use too much memory).
      while (true) {
        if (outputIterator.hasNext()) {
          return outputIterator.next();
        } else if (inputIterator.hasNext()) {
          clearOutput();
          // grab the next element and process it.
          doFnRunner.processElement(inputIterator.next());
          outputIterator = getOutputIterator();
        } else if (timerDataIterator.hasNext()) {
          clearOutput();
          fireTimer(timerDataIterator.next());
          outputIterator = getOutputIterator();
        } else {
          // no more input to consume, but finishBundle can produce more output
          if (!calledFinish) {
            clearOutput();
            calledFinish = true;
            doFnRunner.finishBundle();
            // teardown DoFn.
            DoFnInvokers.invokerFor(doFn).invokeTeardown();
            outputIterator = getOutputIterator();
            continue; // try to consume outputIterator from start of loop
          }
          return endOfData();
        }
      }
    }

    private void fireTimer(
        TimerInternals.TimerData timer) {
      StateNamespace namespace = timer.getNamespace();
      checkArgument(namespace instanceof StateNamespaces.WindowNamespace);
      BoundedWindow window = ((StateNamespaces.WindowNamespace) namespace).getWindow();
      doFnRunner.onTimer(timer.getTimerId(), window, timer.getTimestamp(), timer.getDomain());
    }

  }
}
