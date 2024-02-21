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
package org.apache.beam.fn.harness;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables.getOnlyElement;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.HandlesSplits.SplitResult;
import org.apache.beam.fn.harness.control.BundleProgressReporter;
import org.apache.beam.fn.harness.state.BeamFnStateClient;
import org.apache.beam.fn.harness.state.StateBackedIterable.StateBackedIterableTranslationContext;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleSplitRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleSplitRequest.DesiredSplit;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleSplitResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.RemoteGrpcPort;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants.Urns;
import org.apache.beam.runners.core.metrics.MonitoringInfoEncodings;
import org.apache.beam.runners.core.metrics.ShortIdMap;
import org.apache.beam.runners.core.metrics.SimpleMonitoringInfoBuilder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.RemoteGrpcPortRead;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.CoderTranslation;
import org.apache.beam.sdk.util.construction.RehydratedComponents;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registers as a consumer for data over the Beam Fn API. Multiplexes any received data to all
 * receivers in a specified output map.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class BeamFnDataReadRunner<OutputT> {

  private static final Logger LOG = LoggerFactory.getLogger(BeamFnDataReadRunner.class);

  /** A registrar which provides a factory to handle reading from the Fn Api Data Plane. */
  @AutoService(PTransformRunnerFactory.Registrar.class)
  public static class Registrar implements PTransformRunnerFactory.Registrar {

    @Override
    public Map<String, PTransformRunnerFactory> getPTransformRunnerFactories() {
      return ImmutableMap.of(RemoteGrpcPortRead.URN, new Factory());
    }
  }

  /** A factory for {@link BeamFnDataReadRunner}s. */
  static class Factory<OutputT> implements PTransformRunnerFactory<BeamFnDataReadRunner<OutputT>> {

    @Override
    public BeamFnDataReadRunner<OutputT> createRunnerForPTransform(Context context)
        throws IOException {

      FnDataReceiver<WindowedValue<OutputT>> consumer =
          context.getPCollectionConsumer(
              getOnlyElement(context.getPTransform().getOutputsMap().values()));

      BeamFnDataReadRunner<OutputT> runner =
          new BeamFnDataReadRunner<>(
              context.getShortIdMap(),
              context.getBundleCacheSupplier(),
              context.getPTransformId(),
              context.getPTransform(),
              context.getProcessBundleInstructionIdSupplier(),
              context.getCoders(),
              context.getBeamFnStateClient(),
              context::addBundleProgressReporter,
              consumer);
      context.addIncomingDataEndpoint(
          runner.apiServiceDescriptor, runner.coder, runner::forwardElementToConsumer);
      context.addFinishBundleFunction(runner::blockTillReadFinishes);
      context.addResetFunction(runner::reset);
      return runner;
    }
  }

  private final String pTransformId;
  private final Endpoints.ApiServiceDescriptor apiServiceDescriptor;
  private final FnDataReceiver<WindowedValue<OutputT>> consumer;
  private final Supplier<String> processBundleInstructionIdSupplier;
  private final Coder<WindowedValue<OutputT>> coder;
  private final String dataChannelReadIndexShortId;

  private final Object splittingLock = new Object();
  // 0-based index of the current element being processed. -1 if we have yet to process an element.
  // stopIndex if we are done processing.
  private long index;
  // 0-based index of the first element to not process, aka the first element of the residual
  private long stopIndex;

  BeamFnDataReadRunner(
      ShortIdMap shortIdMap,
      Supplier<Cache<?, ?>> cache,
      String pTransformId,
      RunnerApi.PTransform grpcReadNode,
      Supplier<String> processBundleInstructionIdSupplier,
      Map<String, RunnerApi.Coder> coders,
      BeamFnStateClient beamFnStateClient,
      Consumer<BundleProgressReporter> addBundleProgressReporter,
      FnDataReceiver<WindowedValue<OutputT>> consumer)
      throws IOException {
    this.pTransformId = pTransformId;
    RemoteGrpcPort port = RemoteGrpcPortRead.fromPTransform(grpcReadNode).getPort();
    this.apiServiceDescriptor = port.getApiServiceDescriptor();
    this.processBundleInstructionIdSupplier = processBundleInstructionIdSupplier;
    this.consumer = consumer;

    RehydratedComponents components =
        RehydratedComponents.forComponents(Components.newBuilder().putAllCoders(coders).build());
    this.coder =
        (Coder<WindowedValue<OutputT>>)
            CoderTranslation.fromProto(
                coders.get(port.getCoderId()),
                components,
                new StateBackedIterableTranslationContext() {
                  @Override
                  public Supplier<Cache<?, ?>> getCache() {
                    return cache;
                  }

                  @Override
                  public BeamFnStateClient getStateClient() {
                    return beamFnStateClient;
                  }

                  @Override
                  public Supplier<String> getCurrentInstructionId() {
                    return processBundleInstructionIdSupplier;
                  }
                });

    dataChannelReadIndexShortId =
        shortIdMap.getOrCreateShortId(
            new SimpleMonitoringInfoBuilder()
                .setUrn(Urns.DATA_CHANNEL_READ_INDEX)
                .setType(MonitoringInfoConstants.TypeUrns.SUM_INT64_TYPE)
                .setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, pTransformId)
                .build());
    addBundleProgressReporter.accept(
        new BundleProgressReporter() {
          @Override
          public void updateIntermediateMonitoringData(Map<String, ByteString> monitoringData) {
            synchronized (splittingLock) {
              monitoringData.put(
                  dataChannelReadIndexShortId, MonitoringInfoEncodings.encodeInt64Counter(index));
            }
          }

          @Override
          public void updateFinalMonitoringData(Map<String, ByteString> monitoringData) {
            /**
             * We report the data channel read index on bundle completion allowing runners to
             * perform a sanity check to ensure that the index aligns with any splitting that had
             * occurred preventing missing or duplicate processing of data.
             *
             * <p>We are guaranteed to have the latest version of index since {@link
             * BeamFnDataReadRunner#blockTillReadFinishes} will have been invoked on the main bundle
             * processing thread synchronizing on {@code splittingLock}.
             */
            monitoringData.put(
                dataChannelReadIndexShortId, MonitoringInfoEncodings.encodeInt64Counter(index));
          }

          @Override
          public void reset() {
            // no-op
          }
        });

    clearSplitIndices();
  }

  public void forwardElementToConsumer(WindowedValue<OutputT> element) throws Exception {
    synchronized (splittingLock) {
      if (index == stopIndex - 1) {
        return;
      }
      index += 1;
    }
    consumer.accept(element);
  }

  public void trySplit(
      ProcessBundleSplitRequest request, ProcessBundleSplitResponse.Builder response) {
    DesiredSplit desiredSplit = request.getDesiredSplitsMap().get(pTransformId);
    if (desiredSplit == null) {
      return;
    }

    long totalBufferSize = desiredSplit.getEstimatedInputElements();
    List<Long> allowedSplitPoints = new ArrayList<>(desiredSplit.getAllowedSplitPointsList());

    HandlesSplits splittingConsumer = null;
    if (consumer instanceof HandlesSplits) {
      splittingConsumer = ((HandlesSplits) consumer);
    }

    synchronized (splittingLock) {
      // Don't attempt to split if we are already done since there isn't a meaningful split we can
      // provide.
      if (index == stopIndex) {
        return;
      }
      // Since we hold the splittingLock, we guarantee that we will not pass the next element
      // to the downstream consumer. We still have a race where the downstream consumer may
      // have yet to see the element or has completed processing the element by the time
      // we ask it to split (even after we have asked for its progress).

      // If the split request we received was delayed we it may be for a previous bundle.
      // Ensure we're processing a split for *this* bundle.  This check is done under the lock
      // to make sure reset() is not called concurrently in case the bundle processor is
      // being released.
      if (!request.getInstructionId().equals(processBundleInstructionIdSupplier.get())) {
        return;
      }

      // If the split request we received was delayed and is less then the known number of elements
      // then use "index + 1" as the total size. Similarly, if we have already split and the
      // split request is bounded incorrectly, use the stop index as the upper bound.
      if (totalBufferSize < index + 1) {
        totalBufferSize = index + 1;
      } else if (totalBufferSize > stopIndex) {
        totalBufferSize = stopIndex;
      }

      // In the case where we have yet to process an element, set the current element progress to 1.
      double currentElementProgress = 1;

      // If we have started processing at least one element, attempt to get the downstream
      // progress defaulting to 0.5 if no progress was able to get fetched.
      if (index >= 0) {
        if (splittingConsumer != null) {
          currentElementProgress = splittingConsumer.getProgress();
        } else {
          currentElementProgress = 0.5;
        }
      }

      // Now figure out where to split.
      //
      // The units here (except for keepOfElementRemainder) are all in terms of number or
      // (possibly fractional) elements.

      // Compute the amount of "remaining" work that we know of.
      double remainder = totalBufferSize - index - currentElementProgress;
      // Compute the number of elements (including fractional elements) that we should "keep".
      double keep = remainder * desiredSplit.getFractionOfRemainder();

      // If the downstream operator says the progress is less than 1 then the element could be
      // splittable.
      if (currentElementProgress < 1) {
        // See if the amount we need to keep falls within the current element's remainder and if
        // so, attempt to split it.
        double keepOfElementRemainder = keep / (1 - currentElementProgress);
        // If both index and index are allowed split point, we can split at index.
        if (keepOfElementRemainder < 1
            && isValidSplitPoint(allowedSplitPoints, index)
            && isValidSplitPoint(allowedSplitPoints, index + 1)) {
          SplitResult splitResult =
              splittingConsumer != null ? splittingConsumer.trySplit(keepOfElementRemainder) : null;
          if (splitResult != null) {
            stopIndex = index + 1;
            response
                .addAllPrimaryRoots(splitResult.getPrimaryRoots())
                .addAllResidualRoots(splitResult.getResidualRoots())
                .addChannelSplitsBuilder()
                .setLastPrimaryElement(index - 1)
                .setFirstResidualElement(stopIndex);
            return;
          }
        }
      }

      // Otherwise, split at the closest allowed element boundary.
      long newStopIndex = index + Math.max(1, Math.round(currentElementProgress + keep));
      if (!isValidSplitPoint(allowedSplitPoints, newStopIndex)) {
        // Choose the closest allowed split point.
        Collections.sort(allowedSplitPoints);
        int closestSplitPointIndex =
            -(Collections.binarySearch(allowedSplitPoints, newStopIndex) + 1);
        if (closestSplitPointIndex == 0) {
          newStopIndex = allowedSplitPoints.get(0);
        } else if (closestSplitPointIndex == allowedSplitPoints.size()) {
          newStopIndex = allowedSplitPoints.get(closestSplitPointIndex - 1);
        } else {
          long prevPoint = allowedSplitPoints.get(closestSplitPointIndex - 1);
          long nextPoint = allowedSplitPoints.get(closestSplitPointIndex);
          if (index < prevPoint && newStopIndex - prevPoint < nextPoint - newStopIndex) {
            newStopIndex = prevPoint;
          } else {
            newStopIndex = nextPoint;
          }
        }
      }
      if (newStopIndex < stopIndex && newStopIndex > index) {
        stopIndex = newStopIndex;
        response
            .addChannelSplitsBuilder()
            .setLastPrimaryElement(stopIndex - 1)
            .setFirstResidualElement(stopIndex);
        return;
      }
    }
  }

  public void blockTillReadFinishes() throws Exception {
    LOG.debug(
        "Waiting for process bundle instruction {} and transform {} to close.",
        processBundleInstructionIdSupplier.get(),
        pTransformId);
    synchronized (splittingLock) {
      index += 1;
      stopIndex = index;
    }
  }

  public void reset() {
    checkArgument(
        processBundleInstructionIdSupplier.get() == null,
        "Cannot reset an active bundle processor.");
    clearSplitIndices();
  }

  private void clearSplitIndices() {
    synchronized (splittingLock) {
      index = -1;
      stopIndex = Long.MAX_VALUE;
    }
  }

  private boolean isValidSplitPoint(List<Long> allowedSplitPoints, long index) {
    return allowedSplitPoints.isEmpty() || allowedSplitPoints.contains(index);
  }
}
