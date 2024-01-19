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

import static org.apache.beam.sdk.io.gcp.pubsub.TestPubsub.createTopicName;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.pubsub.v1.PushConfig;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.function.ThrowingRunnable;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.SubscriptionPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Suppliers;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Seconds;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test rule which observes elements of the {@link PCollection} and checks whether they match the
 * success criteria.
 *
 * <p>Uses a random temporary Pubsub topic for synchronization.
 */
@SuppressWarnings({
  "nullness", // TODO(https://github.com/apache/beam/issues/20497)
  // TODO(https://github.com/apache/beam/issues/21230): Remove when new version of
  // errorprone is released (2.11.0)
  "unused"
})
public class TestPubsubSignal implements TestRule {
  private static final Logger LOG = LoggerFactory.getLogger(TestPubsubSignal.class);
  private static final String RESULT_TOPIC_NAME = "result";
  private static final String RESULT_SUCCESS_MESSAGE = "SUCCESS";
  private static final String START_TOPIC_NAME = "start";
  private static final String START_SIGNAL_MESSAGE = "START SIGNAL";

  private final TestPubsubOptions pipelineOptions;
  private final String pubsubEndpoint;

  private @Nullable TopicAdminClient topicAdmin = null;
  private @Nullable SubscriptionAdminClient subscriptionAdmin = null;
  private @Nullable TopicPath resultTopicPath = null;
  private @Nullable TopicPath startTopicPath = null;
  private @Nullable SubscriptionPath resultSubscriptionPath = null;
  private @Nullable SubscriptionPath startSubscriptionPath = null;

  /**
   * Creates an instance of this rule.
   *
   * <p>Loads GCP configuration from {@link TestPipelineOptions}.
   */
  public static TestPubsubSignal create() {
    TestPubsubOptions options = TestPipeline.testingPipelineOptions().as(TestPubsubOptions.class);
    return new TestPubsubSignal(options);
  }

  private TestPubsubSignal(TestPubsubOptions pipelineOptions) {
    this.pipelineOptions = pipelineOptions;
    this.pubsubEndpoint = PubsubOptions.targetForRootUrl(this.pipelineOptions.getPubsubRootUrl());
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        if (topicAdmin != null || subscriptionAdmin != null) {
          throw new AssertionError(
              "Pubsub client was not shutdown in previous test. "
                  + "Topic path is'"
                  + resultTopicPath
                  + "'. "
                  + "Current test: "
                  + description.getDisplayName());
        }

        try {
          initializePubsub(description);
          base.evaluate();
        } finally {
          tearDown();
        }
      }
    };
  }

  private void initializePubsub(Description description) throws IOException {
    topicAdmin =
        TopicAdminClient.create(
            TopicAdminSettings.newBuilder()
                .setCredentialsProvider(pipelineOptions::getGcpCredential)
                .setEndpoint(pubsubEndpoint)
                .build());
    subscriptionAdmin =
        SubscriptionAdminClient.create(
            SubscriptionAdminSettings.newBuilder()
                .setCredentialsProvider(pipelineOptions::getGcpCredential)
                .setEndpoint(pubsubEndpoint)
                .build());

    // Example topic name:
    //    integ-test-TestClassName-testMethodName-2018-12-11-23-32-333-<random-long>-result
    TopicPath resultTopicPathTmp =
        PubsubClient.topicPathFromName(
            pipelineOptions.getProject(), createTopicName(description, RESULT_TOPIC_NAME));
    TopicPath startTopicPathTmp =
        PubsubClient.topicPathFromName(
            pipelineOptions.getProject(), createTopicName(description, START_TOPIC_NAME));
    SubscriptionPath resultSubscriptionPathTmp =
        PubsubClient.subscriptionPathFromName(
            pipelineOptions.getProject(),
            "result-subscription-" + String.valueOf(ThreadLocalRandom.current().nextLong()));
    SubscriptionPath startSubscriptionPathTmp =
        PubsubClient.subscriptionPathFromName(
            pipelineOptions.getProject(),
            "start-subscription-" + String.valueOf(ThreadLocalRandom.current().nextLong()));

    // Set variables after successful creation; this signals that they need teardown
    topicAdmin.createTopic(resultTopicPathTmp.getPath());
    resultTopicPath = resultTopicPathTmp;
    topicAdmin.createTopic(startTopicPathTmp.getPath());
    startTopicPath = startTopicPathTmp;
    subscriptionAdmin.createSubscription(
        resultSubscriptionPathTmp.getPath(),
        resultTopicPath.getPath(),
        PushConfig.getDefaultInstance(),
        600);
    resultSubscriptionPath = resultSubscriptionPathTmp;
    subscriptionAdmin.createSubscription(
        startSubscriptionPathTmp.getPath(),
        startTopicPath.getPath(),
        PushConfig.getDefaultInstance(),
        600);
    startSubscriptionPath = startSubscriptionPathTmp;
  }

  private static void logIfThrows(ThrowingRunnable runnable) {
    try {
      runnable.run();
    } catch (Exception e) {
      LOG.error("Failed to clean up resource.", e);
    }
  }

  private void tearDown() {
    if (subscriptionAdmin == null || topicAdmin == null) {
      return;
    }

    if (resultSubscriptionPath != null) {
      logIfThrows(() -> subscriptionAdmin.deleteSubscription(resultSubscriptionPath.getPath()));
    }
    if (startSubscriptionPath != null) {
      logIfThrows(() -> subscriptionAdmin.deleteSubscription(startSubscriptionPath.getPath()));
    }
    if (resultTopicPath != null) {
      logIfThrows(() -> topicAdmin.deleteTopic(resultTopicPath.getPath()));
    }
    if (startTopicPath != null) {
      logIfThrows(() -> topicAdmin.deleteTopic(startTopicPath.getPath()));
    }

    logIfThrows(subscriptionAdmin::close);
    logIfThrows(topicAdmin::close);

    subscriptionAdmin = null;
    topicAdmin = null;

    resultTopicPath = null;
    startTopicPath = null;

    resultSubscriptionPath = null;
    startSubscriptionPath = null;
  }

  /** Outputs a message that the pipeline has started. */
  public PTransform<PBegin, PDone> signalStart() {
    return new PublishStart(startTopicPath);
  }

  /**
   * Outputs a success message when {@code successPredicate} is evaluated to true.
   *
   * <p>{@code successPredicate} is a {@link SerializableFunction} that accepts a set of currently
   * captured events and returns true when the set satisfies the success criteria.
   *
   * <p>If {@code successPredicate} is evaluated to false, then it will be re-evaluated when next
   * event becomes available.
   *
   * <p>If {@code successPredicate} is evaluated to true, then a success will be signaled and {@link
   * #waitForSuccess(Duration)} will unblock.
   *
   * <p>If {@code successPredicate} throws, then failure will be signaled and {@link
   * #waitForSuccess(Duration)} will unblock.
   */
  public <T> PTransform<PCollection<? extends T>, POutput> signalSuccessWhen(
      Coder<T> coder,
      SerializableFunction<T, String> formatter,
      SerializableFunction<Set<T>, Boolean> successPredicate) {

    return new PublishSuccessWhen<>(coder, formatter, successPredicate, resultTopicPath);
  }

  /**
   * Invocation of {@link #signalSuccessWhen(Coder, SerializableFunction, SerializableFunction)}
   * with {@link Object#toString} as the formatter.
   */
  public <T> PTransform<PCollection<? extends T>, POutput> signalSuccessWhen(
      Coder<T> coder, SerializableFunction<Set<T>, Boolean> successPredicate) {

    return signalSuccessWhen(coder, T::toString, successPredicate);
  }

  /**
   * Future that waits for a start signal for {@code duration}.
   *
   * <p>This future must be created before running the pipeline. A subscription must exist prior to
   * the start signal being published, which occurs immediately upon pipeline startup.
   */
  public Supplier<Void> waitForStart(Duration duration) {
    return Suppliers.memoize(
        () -> {
          try {
            String result = pollForResultForDuration(startSubscriptionPath, duration);
            checkState(START_SIGNAL_MESSAGE.equals(result));
            return null;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  /** Wait for a success signal for {@code duration}. */
  public void waitForSuccess(Duration duration) throws IOException {
    String result = pollForResultForDuration(resultSubscriptionPath, duration);
    if (!RESULT_SUCCESS_MESSAGE.equals(result)) {
      throw new AssertionError(result);
    }
  }

  private String pollForResultForDuration(
      SubscriptionPath signalSubscriptionPath, Duration timeoutDuration) throws IOException {

    AtomicReference<String> result = new AtomicReference<>(null);

    MessageReceiver receiver =
        (com.google.pubsub.v1.PubsubMessage message, AckReplyConsumer replyConsumer) -> {
          LOG.info("Received message: {}", message.getData().toStringUtf8());
          // Ignore empty messages
          if (message.getData().isEmpty()) {
            replyConsumer.ack();
          }
          if (result.compareAndSet(null, message.getData().toStringUtf8())) {
            replyConsumer.ack();
          } else {
            replyConsumer.nack();
          }
        };

    Subscriber subscriber =
        Subscriber.newBuilder(signalSubscriptionPath.getPath(), receiver)
            .setCredentialsProvider(pipelineOptions::getGcpCredential)
            .setEndpoint(pubsubEndpoint)
            .build();
    subscriber.startAsync();

    DateTime startTime = new DateTime();
    int timeoutSeconds = timeoutDuration.toStandardSeconds().getSeconds();
    while (result.get() == null
        && Seconds.secondsBetween(startTime, new DateTime()).getSeconds() < timeoutSeconds) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ignored) {
      }
    }

    subscriber.stopAsync();
    subscriber.awaitTerminated();

    if (result.get() == null) {
      throw new AssertionError(
          String.format(
              "Did not receive signal on %s in %ss",
              signalSubscriptionPath, timeoutDuration.getStandardSeconds()));
    }
    return result.get();
  }

  /** {@link PTransform} that signals once when the pipeline has started. */
  static class PublishStart extends PTransform<PBegin, PDone> {
    private final TopicPath startTopicPath;

    PublishStart(TopicPath startTopicPath) {
      this.startTopicPath = startTopicPath;
    }

    @Override
    public PDone expand(PBegin input) {
      return input
          .apply("Start signal", Create.of(START_SIGNAL_MESSAGE))
          .apply(PubsubIO.writeStrings().to(startTopicPath.getPath()));
    }
  }

  /** {@link PTransform} that for validates whether elements seen so far match success criteria. */
  static class PublishSuccessWhen<T> extends PTransform<PCollection<? extends T>, POutput> {
    private final Coder<T> coder;
    private final SerializableFunction<T, String> formatter;
    private final SerializableFunction<Set<T>, Boolean> successPredicate;
    private final TopicPath resultTopicPath;

    PublishSuccessWhen(
        Coder<T> coder,
        SerializableFunction<T, String> formatter,
        SerializableFunction<Set<T>, Boolean> successPredicate,
        TopicPath resultTopicPath) {

      this.coder = coder;
      this.formatter = formatter;
      this.successPredicate = successPredicate;
      this.resultTopicPath = resultTopicPath;
    }

    @Override
    public POutput expand(PCollection<? extends T> input) {
      return input
          // assign a dummy key and global window,
          // this is needed to accumulate all observed events in the same state cell
          .apply(Window.into(new GlobalWindows()))
          .apply(WithKeys.of("dummyKey"))
          .apply(
              "checkAllEventsForSuccess",
              ParDo.of(new StatefulPredicateCheck<>(coder, formatter, successPredicate)))
          // signal the success/failure to the result topic
          .apply("publishSuccess", PubsubIO.writeStrings().to(resultTopicPath.getPath()));
    }
  }

  /**
   * Stateful {@link DoFn} which caches the elements it sees and checks whether they satisfy the
   * predicate.
   *
   * <p>When predicate is satisfied outputs "SUCCESS". If predicate throws exception, outputs
   * "FAILURE".
   */
  static class StatefulPredicateCheck<T> extends DoFn<KV<String, ? extends T>, String> {
    private SerializableFunction<Set<T>, Boolean> successPredicate;
    // keep all events seen so far in the state cell

    private static final String SEEN_EVENTS = "seenEvents";

    @StateId(SEEN_EVENTS)
    private final StateSpec<BagState<T>> seenEvents;

    StatefulPredicateCheck(
        Coder<T> coder,
        SerializableFunction<T, String> formatter,
        SerializableFunction<Set<T>, Boolean> successPredicate) {
      this.seenEvents = StateSpecs.bag(coder);
      this.successPredicate = successPredicate;
    }

    @ProcessElement
    public void processElement(
        ProcessContext context, @StateId(SEEN_EVENTS) BagState<T> seenEvents) {

      seenEvents.add(context.element().getValue());
      ImmutableSet<T> eventsSoFar = ImmutableSet.copyOf(seenEvents.read());

      // check if all elements seen so far satisfy the success predicate
      try {
        if (successPredicate.apply(eventsSoFar)) {
          LOG.info("Predicate has been satisfied. Sending SUCCESS message.");
          context.output("SUCCESS");
        }
      } catch (Throwable e) {
        LOG.error("Error while applying predicate.", e);
        context.output("FAILURE: " + e.getMessage());
      }
    }
  }
}
