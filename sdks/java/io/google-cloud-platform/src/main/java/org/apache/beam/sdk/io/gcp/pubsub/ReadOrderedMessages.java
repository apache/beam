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
package org.apache.beam.sdk.io.gcp.pubsub;

import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadOrderedMessages extends PTransform<PBegin, PCollection<KV<byte[], byte[]>>> {

  private static final Logger LOG = LoggerFactory.getLogger(ReadOrderedMessages.class);

  public static final Integer BATCH_SIZE = 100;
  public static final Duration POLLING_INTERVAL = Duration.standardSeconds(2);
  private final String subscription;
  private final String project;

  @Nullable private transient SubscriberStub pubsubStub = null;

  private final Integer partitions;

  public ReadOrderedMessages(String subscription, String project, Integer partitions) {
    this.subscription = subscription;
    this.project = project;
    this.partitions = partitions;
  }

  @Override
  public PCollection<KV<byte[], byte[]>> expand(PBegin input) {
    return input
        .getPipeline()
        .apply(Create.of(0))
        .apply(
            FlatMapElements.into(
                    TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.integers()))
                .via(
                    in ->
                        IntStream.range(1, partitions)
                            .boxed()
                            .map(i -> KV.of(i, i))
                            .collect(Collectors.toList())))
        .apply(ParDo.of(new ConsumeOrderedPubsubPartitions()))
        .apply(Reshuffle.of());
  }

  private class ConsumeOrderedPubsubPartitions
      extends DoFn<KV<Integer, Integer>, KV<byte[], byte[]>> {

    @SuppressWarnings("unused")
    @TimerId("polling")
    private final TimerSpec expirySpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    @SuppressWarnings("unused")
    @StateId("toack")
    private final StateSpec<BagState<String>> messagesToAck = StateSpecs.bag();

    private SubscriberStub getPubsubStub() throws IOException {
      if (pubsubStub == null) {
        SubscriberStubSettings subscriberStubSettings =
            SubscriberStubSettings.newBuilder()
                .setTransportChannelProvider(
                    SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                        .setMaxInboundMessageSize(20 * 1024 * 1024) // 20MB (maximum message size).
                        .build())
                .build();

        pubsubStub = GrpcSubscriberStub.create(subscriberStubSettings);
      }
      return pubsubStub;
    }

    private void consumeMessages(
        OutputReceiver<KV<byte[], byte[]>> receiver, BagState<String> toAckState)
        throws IOException {
      String subscriptionName = ProjectSubscriptionName.format(project, subscription);
      PullRequest pullRequest =
          PullRequest.newBuilder()
              .setMaxMessages(BATCH_SIZE)
              .setSubscription(subscriptionName)
              .build();

      while (true) {
        LOG.debug("Pulling messages with config: {}", pullRequest);
        PullResponse pullResponse = getPubsubStub().pullCallable().call(pullRequest);
        LOG.debug("Received {} messages", pullResponse.getReceivedMessagesList().size());
        for (ReceivedMessage message : pullResponse.getReceivedMessagesList()) {
          receiver.output(
              KV.of(
                  message.getMessage().getOrderingKeyBytes().toByteArray(),
                  message.getMessage().getData().toByteArray()));
          toAckState.add(message.getAckId());
        }
        // If we have pulled less than BATCH_SIZE, it means that we can take a break
        // and restart pulling in the next cycle.
        if (pullResponse.getReceivedMessagesList().size() <= BATCH_SIZE) {
          break;
        }
      }
    }

    private void ackMessages(BagState<String> toAckState) throws IOException {
      List<String> toack = Lists.newArrayList(toAckState.read());
      if (toack.size() == 0) {
        return;
      }
      LOG.debug("Acking {} messages.", toack);
      getPubsubStub()
          .acknowledgeCallable()
          .call(
              AcknowledgeRequest.newBuilder()
                  .setSubscription(ProjectSubscriptionName.format(project, subscription))
                  .addAllAckIds(toack)
                  .build());
      toAckState.clear();
    }

    @ProcessElement
    public void process(
        OutputReceiver<KV<byte[], byte[]>> receiver,
        @Element KV<Integer, Integer> elm,
        @TimerId("polling") Timer pollingTimer,
        @StateId("toack") BagState<String> toAckState)
        throws IOException {
      ackMessages(toAckState);
      consumeMessages(receiver, toAckState);
      LOG.debug(
          "Acklist for partition {} has {} elements",
          elm.getKey(),
          Lists.newArrayList(toAckState.read()).size());
      pollingTimer.offset(POLLING_INTERVAL).setRelative();
    }

    @OnTimer("polling")
    public void onPollingTimer(
        @TimerId("polling") Timer pollingTimer,
        OutputReceiver<KV<byte[], byte[]>> receiver,
        @StateId("toack") BagState<String> toAckState,
        @Key Integer key)
        throws IOException {
      ackMessages(toAckState);
      consumeMessages(receiver, toAckState);
      LOG.debug(
          "Acklist for partition {} has {} elements",
          key,
          Lists.newArrayList(toAckState.read()).size());
      pollingTimer.offset(POLLING_INTERVAL).setRelative();
    }
  }
}
