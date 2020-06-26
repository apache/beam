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
package org.apache.beam.runners.twister2.translators.functions;

import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.RecordCollector;
import java.io.ObjectStreamException;
import java.util.Collection;
import java.util.Iterator;
import java.util.logging.Logger;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.SdkComponents;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.core.construction.WindowingStrategyTranslation;
import org.apache.beam.runners.twister2.utils.Twister2AssignContext;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.InvalidProtocolBufferException;

/** Assign Windows function. */
public class AssignWindowsFunction<T>
    implements ComputeCollectorFunc<WindowedValue<T>, Iterator<WindowedValue<T>>> {
  private static final Logger LOG = Logger.getLogger(AssignWindowsFunction.class.getName());

  private transient boolean isInitialized = false;
  private transient WindowFn<T, BoundedWindow> windowFn;
  private byte[] windowFnBytes;
  private transient PipelineOptions options;
  private String serializedOptions;

  public AssignWindowsFunction() {
    // non arg constructor needed for kryo
    this.isInitialized = false;
    this.options = null;
  }

  public AssignWindowsFunction(WindowFn<T, BoundedWindow> windowFn, PipelineOptions options) {
    this.windowFn = windowFn;
    SdkComponents components = SdkComponents.create();
    this.options = options;
    this.serializedOptions = new SerializablePipelineOptions(options).toString();
    components.registerEnvironment(
        Environments.createOrGetDefaultEnvironment(options.as(PortablePipelineOptions.class)));
    RunnerApi.FunctionSpec windowFnProto =
        WindowingStrategyTranslation.toProto(windowFn, components);
    windowFnBytes = windowFnProto.toByteArray();
  }

  @Override
  public void compute(Iterator<WindowedValue<T>> input, RecordCollector<WindowedValue<T>> output) {
    WindowedValue<T> element;
    try {
      while (input.hasNext()) {
        element = input.next();
        Collection<BoundedWindow> windows =
            windowFn.assignWindows(new Twister2AssignContext<>(windowFn, element));

        for (BoundedWindow window : windows) {
          output.collect(
              WindowedValue.of(
                  element.getValue(), element.getTimestamp(), window, element.getPane()));
        }
      }
    } catch (Exception e) {
      LOG.info(e.getMessage());
    }
  }

  @Override
  public void prepare(TSetContext context) {
    initTransient();
  }

  /**
   * Method used to initialize the transient variables that were sent over as byte arrays or proto
   * buffers.
   */
  private void initTransient() {
    if (isInitialized) {
      return;
    }
    options = new SerializablePipelineOptions(serializedOptions).get();

    try {
      RunnerApi.FunctionSpec windowFnProto = RunnerApi.FunctionSpec.parseFrom(windowFnBytes);

      windowFn =
          (WindowFn<T, BoundedWindow>)
              WindowingStrategyTranslation.windowFnFromProto(windowFnProto);
    } catch (InvalidProtocolBufferException e) {
      LOG.info(e.getMessage());
    }
    this.isInitialized = true;
  }

  protected Object readResolve() throws ObjectStreamException {
    return this;
  }
}
