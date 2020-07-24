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

import static org.apache.beam.runners.dataflow.DataflowRunner.StreamingPCollectionViewWriterFn;
import static org.apache.beam.runners.dataflow.util.Structs.getBytes;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.services.dataflow.model.SideInputInfo;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.dataflow.BatchStatefulParDoOverrides;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoFn;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.DoFnInfo;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link ParDoFnFactory} to create instances of user {@link GroupAlsoByWindowFn} according to
 * specifications from the Dataflow service.
 */
class UserParDoFnFactory implements ParDoFnFactory {
  static UserParDoFnFactory createDefault() {
    return new UserParDoFnFactory(new UserDoFnExtractor(), SimpleDoFnRunnerFactory.INSTANCE);
  }

  interface DoFnExtractor {
    DoFnInfo<?, ?> getDoFnInfo(CloudObject cloudUserFn) throws Exception;
  }

  private static class UserDoFnExtractor implements DoFnExtractor {
    @Override
    public DoFnInfo<?, ?> getDoFnInfo(CloudObject cloudUserFn) {
      return (DoFnInfo<?, ?>)
          SerializableUtils.deserializeFromByteArray(
              getBytes(cloudUserFn, PropertyNames.SERIALIZED_FN), "Serialized DoFnInfo");
    }
  }

  // Currently preforms no cleanup to avoid having to add teardown listeners, etc. Reuses DoFns
  // for the life of the worker.
  private final Cache<String, DoFnInstanceManager> fnCache = CacheBuilder.newBuilder().build();

  private final DoFnExtractor doFnExtractor;
  private final DoFnRunnerFactory runnerFactory;

  UserParDoFnFactory(DoFnExtractor doFnExtractor, DoFnRunnerFactory runnerFactory) {
    this.doFnExtractor = doFnExtractor;
    this.runnerFactory = runnerFactory;
  }

  @Override
  public ParDoFn create(
      PipelineOptions options,
      CloudObject cloudUserFn,
      @Nullable List<SideInputInfo> sideInputInfos,
      TupleTag<?> mainOutputTag,
      Map<TupleTag<?>, Integer> outputTupleTagsToReceiverIndices,
      DataflowExecutionContext<?> executionContext,
      DataflowOperationContext operationContext)
      throws Exception {

    DoFnInstanceManager instanceManager =
        fnCache.get(
            operationContext.nameContext().systemName(),
            () -> DoFnInstanceManagers.cloningPool(doFnExtractor.getDoFnInfo(cloudUserFn)));

    DoFnInfo<?, ?> doFnInfo = instanceManager.peek();

    DataflowExecutionContext.DataflowStepContext stepContext =
        executionContext.getStepContext(operationContext);

    Iterable<PCollectionView<?>> sideInputViews = doFnInfo.getSideInputViews();
    SideInputReader sideInputReader =
        executionContext.getSideInputReader(sideInputInfos, sideInputViews, operationContext);

    if (doFnInfo.getDoFn() instanceof BatchStatefulParDoOverrides.BatchStatefulDoFn) {
      // HACK: BatchStatefulDoFn is a class from DataflowRunner's overrides
      // that just instructs the worker to execute it differently. This will
      // be replaced by metadata in the Runner API payload
      BatchStatefulParDoOverrides.BatchStatefulDoFn fn =
          (BatchStatefulParDoOverrides.BatchStatefulDoFn) doFnInfo.getDoFn();
      DoFn underlyingFn = fn.getUnderlyingDoFn();

      return new BatchModeUngroupingParDoFn(
          (BatchModeExecutionContext.StepContext) stepContext,
          new SimpleParDoFn(
              options,
              DoFnInstanceManagers.singleInstance(doFnInfo.withFn(underlyingFn)),
              sideInputReader,
              doFnInfo.getMainOutput(),
              outputTupleTagsToReceiverIndices,
              stepContext,
              operationContext,
              doFnInfo.getDoFnSchemaInformation(),
              doFnInfo.getSideInputMapping(),
              runnerFactory));

    } else if (doFnInfo.getDoFn() instanceof StreamingPCollectionViewWriterFn) {
      // HACK: StreamingPCollectionViewWriterFn is a class from
      // DataflowPipelineTranslator. Using the class as an indicator is a migration path
      // to simply having an indicator string.

      checkArgument(
          stepContext instanceof StreamingModeExecutionContext.StreamingModeStepContext,
          "stepContext must be a StreamingModeStepContext to use StreamingPCollectionViewWriterFn");
      DataflowRunner.StreamingPCollectionViewWriterFn<Object> writerFn =
          (StreamingPCollectionViewWriterFn<Object>) doFnInfo.getDoFn();
      return new StreamingPCollectionViewWriterParDoFn(
          (StreamingModeExecutionContext.StreamingModeStepContext) stepContext,
          writerFn.getView().getTagInternal(),
          writerFn.getDataCoder(),
          (Coder<BoundedWindow>) doFnInfo.getWindowingStrategy().getWindowFn().windowCoder());
    } else {
      return new SimpleParDoFn(
          options,
          instanceManager,
          sideInputReader,
          doFnInfo.getMainOutput(),
          outputTupleTagsToReceiverIndices,
          stepContext,
          operationContext,
          doFnInfo.getDoFnSchemaInformation(),
          doFnInfo.getSideInputMapping(),
          runnerFactory);
    }
  }
}
