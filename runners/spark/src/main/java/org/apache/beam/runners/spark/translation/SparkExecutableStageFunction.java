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

import java.io.IOException;
import java.io.Serializable;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleProgressResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey.TypeCase;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.fnexecution.control.BundleProgressHandler;
import org.apache.beam.runners.fnexecution.control.DefaultJobBundleFactory;
import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.runners.fnexecution.control.OutputReceiverFactory;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers;
import org.apache.beam.runners.fnexecution.translation.BatchSideInputHandlerFactory;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.metrics.MetricsContainerStepMapAccumulator;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * Spark function that passes its input through an SDK-executed {@link
 * org.apache.beam.runners.core.construction.graph.ExecutableStage}.
 *
 * <p>The output of this operation is a multiplexed {@link Dataset} whose elements are tagged with a
 * union coder. The coder's tags are determined by {@link SparkExecutableStageFunction#outputMap}.
 * The resulting data set should be further processed by a {@link
 * SparkExecutableStageExtractionFunction}.
 */
class SparkExecutableStageFunction<InputT, SideInputT>
    implements FlatMapFunction<Iterator<WindowedValue<InputT>>, RawUnionValue> {

  private static final Logger LOG = LoggerFactory.getLogger(SparkExecutableStageFunction.class);

  private final RunnerApi.ExecutableStagePayload stagePayload;
  private final Map<String, Integer> outputMap;
  private final JobBundleFactoryCreator jobBundleFactoryCreator;
  // map from pCollection id to tuple of serialized bytes and coder to decode the bytes
  private final Map<String, Tuple2<Broadcast<List<byte[]>>, WindowedValueCoder<SideInputT>>>
      sideInputs;
  private final MetricsContainerStepMapAccumulator metricsAccumulator;

  SparkExecutableStageFunction(
      RunnerApi.ExecutableStagePayload stagePayload,
      JobInfo jobInfo,
      Map<String, Integer> outputMap,
      Map<String, Tuple2<Broadcast<List<byte[]>>, WindowedValueCoder<SideInputT>>> sideInputs,
      MetricsContainerStepMapAccumulator metricsAccumulator) {
    this(
        stagePayload,
        outputMap,
        () -> DefaultJobBundleFactory.create(jobInfo),
        sideInputs,
        metricsAccumulator);
  }

  SparkExecutableStageFunction(
      RunnerApi.ExecutableStagePayload stagePayload,
      Map<String, Integer> outputMap,
      JobBundleFactoryCreator jobBundleFactoryCreator,
      Map<String, Tuple2<Broadcast<List<byte[]>>, WindowedValueCoder<SideInputT>>> sideInputs,
      MetricsContainerStepMapAccumulator metricsAccumulator) {
    this.stagePayload = stagePayload;
    this.outputMap = outputMap;
    this.jobBundleFactoryCreator = jobBundleFactoryCreator;
    this.sideInputs = sideInputs;
    this.metricsAccumulator = metricsAccumulator;
  }

  @Override
  public Iterator<RawUnionValue> call(Iterator<WindowedValue<InputT>> inputs) throws Exception {
    JobBundleFactory jobBundleFactory = jobBundleFactoryCreator.create();
    ExecutableStage executableStage = ExecutableStage.fromPayload(stagePayload);
    try (StageBundleFactory stageBundleFactory = jobBundleFactory.forStage(executableStage)) {
      ConcurrentLinkedQueue<RawUnionValue> collector = new ConcurrentLinkedQueue<>();
      ReceiverFactory receiverFactory = new ReceiverFactory(collector, outputMap);
      StateRequestHandler stateRequestHandler =
          getStateRequestHandler(executableStage, stageBundleFactory.getProcessBundleDescriptor());
      String stageName = stagePayload.getInput();
      MetricsContainerImpl container = metricsAccumulator.value().getContainer(stageName);
      BundleProgressHandler bundleProgressHandler =
          new BundleProgressHandler() {
            @Override
            public void onProgress(ProcessBundleProgressResponse progress) {
              container.update(progress.getMonitoringInfosList());
            }

            @Override
            public void onCompleted(ProcessBundleResponse response) {
              container.update(response.getMonitoringInfosList());
            }
          };
      try (RemoteBundle bundle =
          stageBundleFactory.getBundle(
              receiverFactory, stateRequestHandler, bundleProgressHandler)) {
        String inputPCollectionId = executableStage.getInputPCollection().getId();
        FnDataReceiver<WindowedValue<?>> mainReceiver =
            bundle.getInputReceivers().get(inputPCollectionId);
        while (inputs.hasNext()) {
          WindowedValue<InputT> input = inputs.next();
          mainReceiver.accept(input);
        }
      }
      return collector.iterator();
    } catch (Exception e) {
      LOG.error("Spark executable stage fn terminated with exception: ", e);
      throw e;
    }
  }

  private StateRequestHandler getStateRequestHandler(
      ExecutableStage executableStage,
      ProcessBundleDescriptors.ExecutableProcessBundleDescriptor processBundleDescriptor) {
    EnumMap<TypeCase, StateRequestHandler> handlerMap = new EnumMap<>(StateKey.TypeCase.class);
    final StateRequestHandler sideInputHandler;
    StateRequestHandlers.SideInputHandlerFactory sideInputHandlerFactory =
        BatchSideInputHandlerFactory.forStage(
            executableStage,
            new BatchSideInputHandlerFactory.SideInputGetter() {
              @Override
              public <T> List<T> getSideInput(String pCollectionId) {
                Tuple2<Broadcast<List<byte[]>>, WindowedValueCoder<SideInputT>> tuple2 =
                    sideInputs.get(pCollectionId);
                Broadcast<List<byte[]>> broadcast = tuple2._1;
                WindowedValueCoder<SideInputT> coder = tuple2._2;
                return (List<T>)
                    broadcast.value().stream()
                        .map(bytes -> CoderHelpers.fromByteArray(bytes, coder))
                        .collect(Collectors.toList());
              }
            });
    try {
      sideInputHandler =
          StateRequestHandlers.forSideInputHandlerFactory(
              ProcessBundleDescriptors.getSideInputs(executableStage), sideInputHandlerFactory);
    } catch (IOException e) {
      throw new RuntimeException("Failed to setup state handler", e);
    }
    handlerMap.put(StateKey.TypeCase.MULTIMAP_SIDE_INPUT, sideInputHandler);
    return StateRequestHandlers.delegateBasedUponType(handlerMap);
  }

  interface JobBundleFactoryCreator extends Serializable {
    JobBundleFactory create();
  }

  /**
   * Receiver factory that wraps outgoing elements with the corresponding union tag for a
   * multiplexed PCollection.
   */
  private static class ReceiverFactory implements OutputReceiverFactory {

    private final ConcurrentLinkedQueue<RawUnionValue> collector;
    private final Map<String, Integer> outputMap;

    ReceiverFactory(
        ConcurrentLinkedQueue<RawUnionValue> collector, Map<String, Integer> outputMap) {
      this.collector = collector;
      this.outputMap = outputMap;
    }

    @Override
    public <OutputT> FnDataReceiver<OutputT> create(String pCollectionId) {
      Integer unionTag = outputMap.get(pCollectionId);
      if (unionTag == null) {
        throw new IllegalStateException(
            String.format(Locale.ENGLISH, "Unknown PCollectionId %s", pCollectionId));
      }
      int tagInt = unionTag;
      return receivedElement -> collector.add(new RawUnionValue(tagInt, receivedElement));
    }
  }
}
