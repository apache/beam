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
package org.apache.beam.sdk.io.gcp.pubsublite;

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.junit.Assert.fail;

import com.google.cloud.pubsublite.AdminClient;
import com.google.cloud.pubsublite.AdminClientSettings;
import com.google.cloud.pubsublite.BacklogLocation;
import com.google.cloud.pubsublite.CloudZone;
import com.google.cloud.pubsublite.Message;
import com.google.cloud.pubsublite.ProjectId;
import com.google.cloud.pubsublite.SubscriptionName;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.TopicName;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.proto.PubSubMessage;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import com.google.cloud.pubsublite.proto.Subscription;
import com.google.cloud.pubsublite.proto.Subscription.DeliveryConfig.DeliveryRequirement;
import com.google.cloud.pubsublite.proto.Topic;
import com.google.cloud.pubsublite.proto.Topic.PartitionConfig.Capacity;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.protobuf.ByteString;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public class ReadWriteIT {
  private static final Logger LOG = LoggerFactory.getLogger(ReadWriteIT.class);
  private static final CloudZone ZONE = CloudZone.parse("us-central1-b");
  private static final int MESSAGE_COUNT = 90;

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  private static ProjectId getProject(PipelineOptions options) {
    return ProjectId.of(checkArgumentNotNull(options.as(GcpOptions.class).getProject()));
  }

  private static String randomName() {
    return "beam_it_resource_" + ThreadLocalRandom.current().nextLong();
  }

  private static AdminClient newAdminClient() {
    return AdminClient.create(AdminClientSettings.newBuilder().setRegion(ZONE.region()).build());
  }

  private final Deque<Runnable> cleanupActions = new ArrayDeque<>();

  private TopicPath createTopic(ProjectId id) throws Exception {
    TopicPath toReturn =
        TopicPath.newBuilder()
            .setProject(id)
            .setLocation(ZONE)
            .setName(TopicName.of(randomName()))
            .build();
    Topic.Builder topic = Topic.newBuilder().setName(toReturn.toString());
    topic
        .getPartitionConfigBuilder()
        .setCount(2)
        .setCapacity(Capacity.newBuilder().setPublishMibPerSec(4).setSubscribeMibPerSec(4));
    topic.getRetentionConfigBuilder().setPerPartitionBytes(30 * (1L << 30));
    cleanupActions.addLast(
        () -> {
          try (AdminClient client = newAdminClient()) {
            client.deleteTopic(toReturn).get();
          } catch (Throwable t) {
            LOG.error("Failed to clean up topic.", t);
          }
        });
    try (AdminClient client = newAdminClient()) {
      client.createTopic(topic.build()).get();
    }
    return toReturn;
  }

  private SubscriptionPath createSubscription(TopicPath topic) throws Exception {
    SubscriptionPath toReturn =
        SubscriptionPath.newBuilder()
            .setProject(topic.project())
            .setLocation(ZONE)
            .setName(SubscriptionName.of(randomName()))
            .build();
    Subscription.Builder subscription = Subscription.newBuilder().setName(toReturn.toString());
    subscription
        .getDeliveryConfigBuilder()
        .setDeliveryRequirement(DeliveryRequirement.DELIVER_IMMEDIATELY);
    subscription.setTopic(topic.toString());
    cleanupActions.addLast(
        () -> {
          try (AdminClient client = newAdminClient()) {
            client.deleteSubscription(toReturn).get();
          } catch (Throwable t) {
            LOG.error("Failed to clean up subscription.", t);
          }
        });
    try (AdminClient client = newAdminClient()) {
      client.createSubscription(subscription.build(), BacklogLocation.BEGINNING).get();
    }
    return toReturn;
  }

  @After
  public void tearDown() {
    while (!cleanupActions.isEmpty()) {
      cleanupActions.removeLast().run();
    }
  }

  // Workaround for BEAM-12867
  // TODO(BEAM-12867): Remove this.
  private static class CustomCreate extends PTransform<PCollection<Void>, PCollection<Integer>> {
    @Override
    public PCollection<Integer> expand(PCollection<Void> input) {
      return input.apply(
          "createIndexes",
          FlatMapElements.via(
              new SimpleFunction<Void, Iterable<Integer>>() {
                @Override
                public Iterable<Integer> apply(Void input) {
                  return IntStream.range(0, MESSAGE_COUNT).boxed().collect(Collectors.toList());
                }
              }));
    }
  }

  public static void writeMessages(TopicPath topicPath, Pipeline pipeline) {
    PCollection<Void> trigger = pipeline.apply(Create.of((Void) null));
    PCollection<Integer> indexes = trigger.apply("createIndexes", new CustomCreate());
    PCollection<PubSubMessage> messages =
        indexes.apply(
            "createMessages",
            MapElements.via(
                new SimpleFunction<Integer, PubSubMessage>(
                    index ->
                        Message.builder()
                            .setData(ByteString.copyFromUtf8(index.toString()))
                            .build()
                            .toProto()) {}));
    // Add UUIDs to messages for later deduplication.
    messages = messages.apply("addUuids", PubsubLiteIO.addUuids());
    messages.apply(
        "writeMessages",
        PubsubLiteIO.write(PublisherOptions.newBuilder().setTopicPath(topicPath).build()));
  }

  public static PCollection<SequencedMessage> readMessages(
      SubscriptionPath subscriptionPath, Pipeline pipeline) {
    PCollection<SequencedMessage> messages =
        pipeline.apply(
            "readMessages",
            PubsubLiteIO.read(
                SubscriberOptions.newBuilder()
                    .setSubscriptionPath(subscriptionPath)
                    // setMinBundleTimeout INTENDED FOR TESTING ONLY
                    // This sacrifices efficiency to make tests run faster. Do not use this in a
                    // real pipeline!
                    .setMinBundleTimeout(Duration.standardSeconds(5))
                    .build()));
    // Deduplicate messages based on the uuids added in PubsubLiteIO.addUuids() when writing.
    return messages.apply(
        "dedupeMessages", PubsubLiteIO.deduplicate(UuidDeduplicationOptions.newBuilder().build()));
  }

  // This static out of band communication is needed to retain serializability.
  @GuardedBy("ReadWriteIT.class")
  private static final List<SequencedMessage> received = new ArrayList<>();

  private static synchronized void addMessageReceived(SequencedMessage message) {
    received.add(message);
  }

  private static synchronized List<SequencedMessage> getTestQuickstartReceived() {
    return ImmutableList.copyOf(received);
  }

  private static PTransform<PCollection<? extends SequencedMessage>, PCollection<Void>>
      collectTestQuickstart() {
    return MapElements.via(
        new SimpleFunction<SequencedMessage, Void>() {
          @Override
          public Void apply(SequencedMessage input) {
            addMessageReceived(input);
            return null;
          }
        });
  }

  @Test
  public void testReadWrite() throws Exception {
    pipeline.getOptions().as(StreamingOptions.class).setStreaming(true);
    pipeline.getOptions().as(TestPipelineOptions.class).setBlockOnRun(false);

    TopicPath topic = createTopic(getProject(pipeline.getOptions()));
    SubscriptionPath subscription = null;
    Exception lastException = null;
    for (int i = 0; i < 30; ++i) {
      // Sleep for topic creation to propagate.
      Thread.sleep(1000);
      try {
        subscription = createSubscription(topic);
      } catch (Exception e) {
        lastException = e;
        LOG.info("Retrying exception on subscription creation.", e);
      }
    }
    if (subscription == null) {
      throw lastException;
    }

    // Publish some messages
    writeMessages(topic, pipeline);

    // Read some messages. They should be deduplicated by the time we see them, so there should be
    // exactly numMessages, one for every index in [0,MESSAGE_COUNT).
    PCollection<SequencedMessage> messages = readMessages(subscription, pipeline);
    messages.apply("messageReceiver", collectTestQuickstart());
    pipeline.run();
    LOG.info("Running!");
    for (int round = 0; round < 120; ++round) {
      Thread.sleep(1000);
      Map<Integer, Integer> receivedCounts = new HashMap<>();
      for (SequencedMessage message : getTestQuickstartReceived()) {
        int id = Integer.parseInt(message.getMessage().getData().toStringUtf8());
        receivedCounts.put(id, receivedCounts.getOrDefault(id, 0) + 1);
      }
      LOG.info("Performing comparison round {}.\n", round);
      boolean done = true;
      List<Integer> missing = new ArrayList<>();
      for (int id = 0; id < MESSAGE_COUNT; id++) {
        int idCount = receivedCounts.getOrDefault(id, 0);
        if (idCount == 0) {
          missing.add(id);
          done = false;
        }
        if (idCount > 1) {
          fail(String.format("Failed to deduplicate message with id %s.", id));
        }
      }
      LOG.info("Still messing messages: {}.\n", missing);
      if (done) {
        return;
      }
    }
    fail(
        String.format(
            "Failed to receive all messages after 2 minutes. Received %s messages.",
            getTestQuickstartReceived().size()));
  }
}
