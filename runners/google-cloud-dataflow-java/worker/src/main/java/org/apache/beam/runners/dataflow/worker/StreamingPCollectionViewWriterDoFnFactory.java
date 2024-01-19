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
package org.apache.beam.runners.dataflow.worker;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.services.dataflow.model.SideInputInfo;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.util.Structs;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext.DataflowStepContext;
import org.apache.beam.runners.dataflow.worker.util.WorkerPropertyNames;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoFn;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.values.TupleTag;

/**
 * A {@link ParDoFnFactory} which constructs {@link StreamingPCollectionViewWriterParDoFn}s used to
 * write side input data.
 *
 * <p>Note that this {@link ParDoFnFactory} may only be used in a streaming context.
 */
public class StreamingPCollectionViewWriterDoFnFactory implements ParDoFnFactory {

  @Override
  public ParDoFn create(
      PipelineOptions options,
      CloudObject cloudUserFn,
      List<SideInputInfo> sideInputInfos,
      TupleTag<?> mainOutputTag,
      Map<TupleTag<?>, Integer> outputTupleTagsToReceiverIndices,
      DataflowExecutionContext<?> executionContext,
      DataflowOperationContext operationContext)
      throws Exception {
    DataflowStepContext stepContext = executionContext.getStepContext(operationContext);
    checkArgument(
        stepContext instanceof StreamingModeExecutionContext.StreamingModeStepContext,
        "stepContext must be a StreamingModeStepContext to use StreamingPCollectionViewWriterFn");

    Coder<?> coder =
        CloudObjects.coderFromCloudObject(
            CloudObject.fromSpec(Structs.getObject(cloudUserFn, PropertyNames.ENCODING)));
    checkState(
        coder instanceof FullWindowedValueCoder,
        "Expected to received an instanceof an %s but got %s",
        FullWindowedValueCoder.class.getSimpleName(),
        coder);
    FullWindowedValueCoder<?> windowedValueCoder = (FullWindowedValueCoder<?>) coder;
    return new StreamingPCollectionViewWriterParDoFn(
        (StreamingModeExecutionContext.StreamingModeStepContext) stepContext,
        new TupleTag<>(Structs.getString(cloudUserFn, WorkerPropertyNames.SIDE_INPUT_ID)),
        (Coder) windowedValueCoder.getValueCoder(),
        (Coder) windowedValueCoder.getWindowCoder());
  }
}
