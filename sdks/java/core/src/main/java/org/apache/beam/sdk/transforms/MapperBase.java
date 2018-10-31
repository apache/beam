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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.Contextful.Fn;
import org.apache.beam.sdk.transforms.Contextful.Fn.Context;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Abstract class providing a base for {@link PTransform}s that map a simple function over elements.
 *
 * <p>Each subclass accepts a user-defined function that takes in {@code InputT} and returns {@code
 * IntermediateT}. Each subclass also defines an {@code emitOutput} function that outputs one or
 * more {@code OutputT} elements based on the input element and result of applying the user-defined
 * function. Each subclass defines some {@code IntermediateT}, accepting a function that transforms
 * input elements to {@code IntermediateT} and overriding the {@code emitOutput} function for
 * handling what should be sent to the output receiver based on input and a particular {@code
 * IntermediateT} value.
 */
abstract class MapperBase<InputT, IntermediateT, OutputT>
    extends PTransform<PCollection<? extends InputT>, PCollection<OutputT>> {
  @Nullable private final transient TypeDescriptor<InputT> inputType;
  @Nullable final transient TypeDescriptor<OutputT> outputType;
  @Nullable private final transient Object originalFnForDisplayData;
  @Nullable private final Contextful<Fn<InputT, IntermediateT>> fn;

  MapperBase(
      @Nullable String name,
      @Nullable Contextful<Fn<InputT, IntermediateT>> fn,
      @Nullable Object originalFnForDisplayData,
      @Nullable TypeDescriptor<InputT> inputType,
      TypeDescriptor<OutputT> outputType) {
    super(name);
    this.fn = fn;
    this.originalFnForDisplayData = originalFnForDisplayData;
    this.inputType = inputType;
    this.outputType = outputType;
  }

  public abstract void emitOutput(
      InputT inputElement, IntermediateT intermediate, OutputReceiver<OutputT> receiver);

  @Override
  public PCollection<OutputT> expand(PCollection<? extends InputT> input) {
    checkNotNull(
        fn,
        "Must specify a function on " + MapperBase.this.getClass().toString() + " using .via()");
    return input.apply(
        ParDo.of(
                new DoFn<InputT, OutputT>() {
                  @ProcessElement
                  public void processElement(
                      @Element InputT element, OutputReceiver<OutputT> receiver, ProcessContext c)
                      throws Exception {
                    IntermediateT intermediate =
                        fn.getClosure().apply(element, Context.wrapProcessContext(c));
                    emitOutput(element, intermediate, receiver);
                  }

                  @Override
                  public TypeDescriptor<InputT> getInputTypeDescriptor() {
                    return inputType;
                  }

                  @Override
                  public TypeDescriptor<OutputT> getOutputTypeDescriptor() {
                    checkState(
                        outputType != null,
                        "%s output type descriptor was null; "
                            + "this probably means that getOutputTypeDescriptor() was called after "
                            + "serialization/deserialization, but it is only available prior to "
                            + "serialization, for constructing a pipeline and inferring coders",
                        MapperBase.this.getClass().getSimpleName());
                    return outputType;
                  }

                  @Override
                  public void populateDisplayData(DisplayData.Builder builder) {
                    builder.delegate(MapperBase.this);
                  }
                })
            .withSideInputs(fn.getRequirements().getSideInputs()));
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);

    // Subclasses can opt to set this null and implement their own display data logic.
    if (originalFnForDisplayData != null) {
      builder.add(DisplayData.item("class", originalFnForDisplayData.getClass()));
    }
    if (originalFnForDisplayData instanceof HasDisplayData) {
      builder.include("fn", (HasDisplayData) originalFnForDisplayData);
    }
  }
}
