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

import java.io.Serializable;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleProgressResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey.TypeCase;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.control.BundleProgressHandler;
import org.apache.beam.runners.fnexecution.control.DefaultJobBundleFactory;
import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.runners.fnexecution.control.OutputReceiverFactory;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark function that passes its input through an SDK-executed {@link
 * org.apache.beam.runners.core.construction.graph.ExecutableStage}.
 *
 * <p>The output of this operation is a multiplexed {@link Dataset} whose elements are tagged with a
 * union coder. The coder's tags are determined by {@link SparkExecutableStageFunction#outputMap}.
 * The resulting data set should be further processed by a {@link
 * SparkExecutableStageExtractionFunction}.
 */
public class SparkExecutableStageFunction<InputT>
    implements FlatMapFunction<Iterator<WindowedValue<InputT>>, RawUnionValue> {

  private static final Logger LOG = LoggerFactory.getLogger(SparkExecutableStageFunction.class);

  private final RunnerApi.ExecutableStagePayload stagePayload;
  private final Map<String, Integer> outputMap;
  private final JobBundleFactoryCreator jobBundleFactoryCreator;

  SparkExecutableStageFunction(
      RunnerApi.ExecutableStagePayload stagePayload,
      JobInfo jobInfo,
      Map<String, Integer> outputMap) {
    this(stagePayload, outputMap, () -> DefaultJobBundleFactory.create(jobInfo));
  }

  SparkExecutableStageFunction(
      RunnerApi.ExecutableStagePayload stagePayload,
      Map<String, Integer> outputMap,
      JobBundleFactoryCreator jobBundleFactoryCreator) {
    this.stagePayload = stagePayload;
    this.outputMap = outputMap;
    this.jobBundleFactoryCreator = jobBundleFactoryCreator;
  }

  @Override
  public Iterator<RawUnionValue> call(Iterator<WindowedValue<InputT>> inputs) throws Exception {
    JobBundleFactory jobBundleFactory = jobBundleFactoryCreator.create();
    ExecutableStage executableStage = ExecutableStage.fromPayload(stagePayload);
    try (StageBundleFactory stageBundleFactory = jobBundleFactory.forStage(executableStage)) {
      ConcurrentLinkedQueue<RawUnionValue> collector = new ConcurrentLinkedQueue<>();
      ReceiverFactory receiverFactory = new ReceiverFactory(collector, outputMap);
      EnumMap<TypeCase, StateRequestHandler> handlers = new EnumMap<>(StateKey.TypeCase.class);
      // TODO add state request handlers
      StateRequestHandler stateRequestHandler =
          StateRequestHandlers.delegateBasedUponType(handlers);
      SparkBundleProgressHandler bundleProgressHandler = new SparkBundleProgressHandler();
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

  private static class SparkBundleProgressHandler implements BundleProgressHandler {

    @Override
    public void onProgress(ProcessBundleProgressResponse progress) {
      // TODO
    }

    @Override
    public void onCompleted(ProcessBundleResponse response) {
      // TODO
    }
  }
}
