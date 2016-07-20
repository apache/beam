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

package org.apache.beam.runners.gearpump.translators.functions;

import org.apache.beam.runners.gearpump.GearpumpPipelineOptions;
import org.apache.beam.runners.gearpump.translators.utils.GearpumpDoFnRunner;
import org.apache.beam.runners.gearpump.translators.utils.NoOpStepContext;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.DoFnRunner;
import org.apache.beam.sdk.util.DoFnRunners;
import org.apache.beam.sdk.util.SideInputReader;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import com.google.api.client.util.Lists;

import org.apache.gearpump.streaming.javaapi.dsl.functions.FlatMapFunction;

import java.util.Iterator;
import java.util.List;

/**
 * Gearpump {@link FlatMapFunction} wrapper over Beam {@link DoFn}.
 */
public class DoFnFunction<InputT, OutputT> implements
    FlatMapFunction<WindowedValue<InputT>, WindowedValue<OutputT>>, DoFnRunners.OutputManager {

  private final TupleTag<OutputT> mainTag = new TupleTag<OutputT>() {
  };
  private final DoFnRunner<InputT, OutputT> doFnRunner;
  private List<WindowedValue<OutputT>> outputs = Lists.newArrayList();

  public DoFnFunction(
      GearpumpPipelineOptions pipelineOptions,
      DoFn<InputT, OutputT> doFn,
      WindowingStrategy<?, ?> windowingStrategy,
      SideInputReader sideInputReader) {
    this.doFnRunner = new GearpumpDoFnRunner<>(
        pipelineOptions,
        doFn,
        sideInputReader,
        this,
        mainTag,
        TupleTagList.empty().getAll(),
        new NoOpStepContext(),
        windowingStrategy
    );
  }

  @Override
  public Iterator<WindowedValue<OutputT>> apply(WindowedValue<InputT> value) {
    outputs = Lists.newArrayList();

    doFnRunner.startBundle();
    doFnRunner.processElement(value);
    doFnRunner.finishBundle();

    return outputs.iterator();
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
    if (mainTag.equals(tag)) {
      outputs.add((WindowedValue<OutputT>) output);
    } else {
      throw new RuntimeException("output is not of main tag");
    }
  }
}
