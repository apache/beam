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
package org.apache.beam.runners.dataflow.worker.windmill.work.provider;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.beam.runners.dataflow.worker.WindmillTimeUtils;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.streaming.Watermarks;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServerStub;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.WorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.GetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.StreamingWorkScheduler;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Supplier;

import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ApplianceWorkProvider extends SingleSourceWorkProvider {
  private static final Logger LOG = LoggerFactory.getLogger(ApplianceWorkProvider.class);

  private final Supplier<Windmill.GetWorkResponse> getWorkFn;

  ApplianceWorkProvider(
      WorkCommitter workCommitter,
      GetDataClient getDataClient,
      HeartbeatSender heartbeatSender,
      StreamingWorkScheduler streamingWorkScheduler,
      Runnable waitForResources,
      Function<String, Optional<ComputationState>> computationStateFetcher,
      Supplier<Windmill.GetWorkResponse> getWorkFn) {
    super(
        workCommitter,
        getDataClient,
        heartbeatSender,
        streamingWorkScheduler,
        waitForResources,
        computationStateFetcher);
    this.getWorkFn = getWorkFn;
  }

  @Override
  protected void dispatchLoop() {
    while (isRunning.get()) {
      waitForResources.run();

      int backoff = 1;
      Windmill.GetWorkResponse workResponse = null;
      do {
        try {
          workResponse = getWorkFn.get();
          if (workResponse.getWorkCount() > 0) {
            break;
          }
        } catch (WindmillServerStub.RpcException e) {
          LOG.warn("GetWork failed, retrying:", e);
        }
        sleepUninterruptibly(backoff, TimeUnit.MILLISECONDS);
        backoff = Math.min(1000, backoff * 2);
      } while (isRunning.get());
      for (final Windmill.ComputationWorkItems computationWork : workResponse.getWorkList()) {
        final String computationId = computationWork.getComputationId();
        Optional<ComputationState> maybeComputationState =
            computationStateFetcher.apply(computationId);
        if (!maybeComputationState.isPresent()) {
          continue;
        }

        final ComputationState computationState = maybeComputationState.get();
        final Instant inputDataWatermark =
            WindmillTimeUtils.windmillToHarnessWatermark(computationWork.getInputDataWatermark());
        Watermarks.Builder watermarks =
            Watermarks.builder()
                .setInputDataWatermark(Preconditions.checkNotNull(inputDataWatermark))
                .setSynchronizedProcessingTime(
                    WindmillTimeUtils.windmillToHarnessWatermark(
                        computationWork.getDependentRealtimeInputWatermark()));

        for (final Windmill.WorkItem workItem : computationWork.getWorkList()) {
          streamingWorkScheduler.scheduleWork(
              computationState,
              workItem,
              watermarks.setOutputDataWatermark(workItem.getOutputDataWatermark()).build(),
              Work.createProcessingContext(
                  computationId, getDataClient, workCommitter::commit, heartbeatSender),
              /* getWorkStreamLatencies= */ Collections.emptyList());
        }
      }
    }
  }

  @Override
  protected String debugName() {
    return "Appliance";
  }
}
