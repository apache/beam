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

import com.google.api.services.dataflow.model.SideInputInfo;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoFn;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/**
 * A factory that dispatches to all known factories in the Dataflow SDK based on the value of {@link
 * CloudObject#getClassName()} for the specified {@code DoFn}.
 */
public class DefaultParDoFnFactory implements ParDoFnFactory {
  private final ImmutableMap<String, ParDoFnFactory> defaultFactories;

  public DefaultParDoFnFactory() {
    defaultFactories =
        ImmutableMap.<String, ParDoFnFactory>builder()
            .put("DoFn", UserParDoFnFactory.createDefault())
            .put("CombineValuesFn", new CombineValuesFnFactory())
            .put("MergeBucketsDoFn", new GroupAlsoByWindowParDoFnFactory())
            .put("AssignBucketsDoFn", new AssignWindowsParDoFnFactory())
            .put("MergeWindowsDoFn", new GroupAlsoByWindowParDoFnFactory())
            .put("AssignWindowsDoFn", new AssignWindowsParDoFnFactory())
            .put("ReifyTimestampAndWindowsDoFn", new ReifyTimestampAndWindowsParDoFnFactory())
            .put("SplittableProcessFn", SplittableProcessFnFactory.createDefault())
            .put("CreateIsmShardKeyAndSortKeyDoFn", new CreateIsmShardKeyAndSortKeyDoFnFactory())
            .put("ToIsmRecordForMultimapDoFn", new ToIsmRecordForMultimapDoFnFactory())
            .put("ValuesDoFn", new ValuesDoFnFactory())
            .put("PairWithConstantKeyDoFn", new PairWithConstantKeyDoFnFactory())
            .put(
                "StreamingPCollectionViewWriterDoFn",
                new StreamingPCollectionViewWriterDoFnFactory())
            .build();
  }

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

    String className = cloudUserFn.getClassName();
    ParDoFnFactory factory = defaultFactories.get(className);

    if (factory == null) {
      throw new Exception("No known ParDoFnFactory for " + className);
    }

    return factory.create(
        options,
        cloudUserFn,
        sideInputInfos,
        mainOutputTag,
        outputTupleTagsToReceiverIndices,
        executionContext,
        operationContext);
  }
}
