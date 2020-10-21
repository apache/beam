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

import static java.util.stream.Collectors.toList;
import static org.apache.beam.sdk.io.gcp.pubsub.TestPubsub.createTopicName;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.IncomingMessage;
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
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Suppliers;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTime;
import org.joda.time.Duration;
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
public class TestPubsubSignal implements TestRule {
  private static final Logger LOG = LoggerFactory.getLogger(TestPubsubSignal.class);
  private static final String RESULT_TOPIC_NAME = "result";
  private static final String RESULT_SUCCESS_MESSAGE = "SUCCESS";
  private static final String START_TOPIC_NAME = "start";
  private static final String START_SIGNAL_MESSAGE = "START SIGNAL";

  private static final String NO_ID_ATTRIBUTE = null;
  private static final String NO_TIMESTAMP_ATTRIBUTE = null;

  PubsubClient pubsub;
  private TestPubsubOptions pipelineOptions;
  private @Nullable TopicPath resultTopicPath = null;
  private @Nullable TopicPath startTopicPath = null;

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
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        if (TestPubsubSignal.this.pubsub != null) {
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
    pubsub =
        PubsubGrpcClient.FACTORY.newClient(
            NO_TIMESTAMP_ATTRIBUTE, NO_ID_ATTRIBUTE, pipelineOptions);

    // Example topic name:
    //    integ-test-TestClassName-testMethodName-2018-12-11-23-32-333-<random-long>-result
    TopicPath resultTopicPathTmp =
        PubsubClient.topicPathFromName(
            pipelineOptions.getProject(), createTopicName(description, RESULT_TOPIC_NAME));
    TopicPath startTopicPathTmp =
        PubsubClient.topicPathFromName(
            pipelineOptions.getProject(), createTopicName(description, START_TOPIC_NAME));

    pubsub.createTopic(resultTopicPathTmp);
    pubsub.createTopic(startTopicPathTmp);

    // Set these after successful creation; this signals that they need teardown
    resultTopicPath = resultTopicPathTmp;
    startTopicPath = startTopicPathTmp;
  }

  private void tearDown() throws IOException {
    if (pubsub == null) {
      return;
    }

    try {
      if (resultTopicPath != null) {
        pubsub.deleteTopic(resultTopicPath);
      }
      if (startTopicPath != null) {
        pubsub.deleteTopic(startTopicPath);
      }
    } finally {
      pubsub.close();
      pubsub = null;
      resultTopicPath = null;
    }
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
  public Supplier<Void> waitForStart(Duration duration) throws IOException {
    SubscriptionPath startSubscriptionPath =
        PubsubClient.subscriptionPathFromName(
            pipelineOptions.getProject(),
            "start-subscription-" + String.valueOf(ThreadLocalRandom.current().nextLong()));

    pubsub.createSubscription(
        startTopicPath, startSubscriptionPath, (int) duration.getStandardSeconds());

    return Suppliers.memoize(
        () -> {
          try {
            String result = pollForResultForDuration(startSubscriptionPath, duration);
            checkState(START_SIGNAL_MESSAGE.equals(result));
            return null;
          } catch (IOException e) {
            throw new RuntimeException(e);
          } finally {
            try {
              pubsub.deleteSubscription(startSubscriptionPath);
            } catch (IOException e) {
              LOG.error(String.format("Leaked PubSub subscription '%s'", startSubscriptionPath));
            }
          }
        });
  }

  /** Wait for a success signal for {@code duration}. */
  public void waitForSuccess(Duration duration) throws IOException {
    SubscriptionPath resultSubscriptionPath =
        PubsubClient.subscriptionPathFromName(
            pipelineOptions.getProject(),
            "result-subscription-" + String.valueOf(ThreadLocalRandom.current().nextLong()));

    pubsub.createSubscription(
        resultTopicPath, resultSubscriptionPath, (int) duration.getStandardSeconds());

    String result = pollForResultForDuration(resultSubscriptionPath, duration);

    try {
      pubsub.deleteSubscription(resultSubscriptionPath);
    } catch (IOException e) {
      LOG.error(String.format("Leaked PubSub subscription '%s'", resultSubscriptionPath));
    }

    if (!RESULT_SUCCESS_MESSAGE.equals(result)) {
      throw new AssertionError(result);
    }
  }

  private String pollForResultForDuration(
      SubscriptionPath signalSubscriptionPath, Duration duration) throws IOException {

    List<PubsubClient.IncomingMessage> signal = null;
    DateTime endPolling = DateTime.now().plus(duration.getMillis());

    do {
      try {
        signal = pubsub.pull(DateTime.now().getMillis(), signalSubscriptionPath, 1, false);
        if (signal.isEmpty()) {
          continue;
        }
        pubsub.acknowledge(
            signalSubscriptionPath, signal.stream().map(IncomingMessage::ackId).collect(toList()));
        break;
      } catch (StatusRuntimeException e) {
        if (!Status.DEADLINE_EXCEEDED.equals(e.getStatus())) {
          LOG.warn(
              "(Will retry) Error while polling {} for signal: {}",
              signalSubscriptionPath,
              e.getStatus());
        }
        sleep(500);
      }
    } while (DateTime.now().isBefore(endPolling));

    if (signal == null || signal.isEmpty()) {
      throw new AssertionError(
          String.format(
              "Did not receive signal on %s in %ss",
              signalSubscriptionPath, duration.getStandardSeconds()));
    }

    return signal.get(0).message().getData().toStringUtf8();
  }

  private void sleep(long t) {
    try {
      Thread.sleep(t);
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
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
    private final SerializableFunction<T, String> formatter;
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
      this.formatter = formatter;
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
          context.output("SUCCESS");
        }
      } catch (Throwable e) {
        context.output("FAILURE: " + e.getMessage());
      }
    }
  }
}
