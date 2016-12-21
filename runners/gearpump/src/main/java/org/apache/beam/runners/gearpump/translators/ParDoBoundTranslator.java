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

package org.apache.beam.runners.gearpump.translators;

import org.apache.beam.runners.gearpump.translators.functions.DoFnFunction;
import org.apache.beam.runners.gearpump.translators.utils.NoOpSideInputReader;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PCollection;

import org.apache.gearpump.streaming.dsl.javaapi.JavaStream;


/**
 * {@link ParDo.Bound} is translated to Gearpump flatMap function
 * with {@link DoFn} wrapped in {@link DoFnFunction}.
 */
public class ParDoBoundTranslator<InputT, OutputT> implements
    TransformTranslator<ParDo.Bound<InputT, OutputT>> {

  @Override
  public void translate(ParDo.Bound<InputT, OutputT> transform, TranslationContext context) {
    DoFn<InputT, OutputT> doFn = transform.getFn();
    PCollection<OutputT> output = context.getOutput(transform);
    WindowingStrategy<?, ?> windowingStrategy = output.getWindowingStrategy();

    DoFnFunction<InputT, OutputT> doFnFunction = new DoFnFunction<>(context.getPipelineOptions(),
        doFn, windowingStrategy, new NoOpSideInputReader());
    JavaStream<WindowedValue<InputT>> inputStream =
        context.getInputStream(context.getInput(transform));
    JavaStream<WindowedValue<OutputT>> outputStream =
        inputStream.flatMap(doFnFunction, transform.getName());

    context.setOutputStream(context.getOutput(transform), outputStream);
  }
}
