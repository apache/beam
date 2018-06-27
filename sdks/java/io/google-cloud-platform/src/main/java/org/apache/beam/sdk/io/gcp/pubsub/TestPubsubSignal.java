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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.apache.beam.sdk.io.gcp.pubsub.TestPubsub.createTopicName;

import com.google.common.collect.ImmutableSet;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.SubscriptionPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Test rule which observes elements of the {@link PCollection} and checks whether they match the
 * success criteria.
 *
 * <p>Uses a random temporary Pubsub topic for synchronization.
 */
public class TestPubsubSignal implements TestRule {
  private static final String TOPIC_FORMAT = "projects/%s/topics/%s-result1";
  private static final String SUBSCRIPTION_FORMAT = "projects/%s/subscriptions/%s";
  private static final String NO_ID_ATTRIBUTE = null;
  private static final String NO_TIMESTAMP_ATTRIBUTE = null;

  PubsubClient pubsub;
  private TestPubsubOptions pipelineOptions;
  private String resultTopicPath;

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
    String resultTopicPathTmp =
        String.format(TOPIC_FORMAT, pipelineOptions.getProject(), createTopicName(description));

    pubsub.createTopic(new TopicPath(resultTopicPathTmp));

    resultTopicPath = resultTopicPathTmp;
  }

  private void tearDown() throws IOException {
    if (pubsub == null) {
      return;
    }

    try {
      if (resultTopicPath != null) {
        pubsub.deleteTopic(new TopicPath(resultTopicPath));
      }
    } finally {
      pubsub.close();
      pubsub = null;
      resultTopicPath = null;
    }
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
      Coder<T> coder, SerializableFunction<Set<T>, Boolean> successPredicate) {

    return new PublishSuccessWhen<>(coder, successPredicate, resultTopicPath);
  }

  /** Wait for a success signal for {@code duration}. */
  public void waitForSuccess(Duration duration) throws IOException {
    SubscriptionPath resultSubscriptionPath =
        new SubscriptionPath(
            String.format(
                SUBSCRIPTION_FORMAT,
                pipelineOptions.getProject(),
                "subscription-" + String.valueOf(ThreadLocalRandom.current().nextLong())));

    pubsub.createSubscription(
        new TopicPath(resultTopicPath),
        resultSubscriptionPath,
        (int) duration.getStandardSeconds());

    String result = pollForResultForDuration(resultSubscriptionPath, duration);

    if (!"SUCCESS".equals(result)) {
      throw new AssertionError(result);
    }
  }

  private String pollForResultForDuration(
      SubscriptionPath resultSubscriptionPath, Duration duration) throws IOException {

    List<PubsubClient.IncomingMessage> result = null;
    DateTime endPolling = DateTime.now().plus(duration.getMillis());

    do {
      try {
        result = pubsub.pull(DateTime.now().getMillis(), resultSubscriptionPath, 1, false);
        pubsub.acknowledge(
            resultSubscriptionPath, result.stream().map(m -> m.ackId).collect(toList()));
        break;
      } catch (StatusRuntimeException e) {
        System.out.println("Error while polling for result: " + e.getStatus());
        sleep(500);
      }
    } while (DateTime.now().isBefore(endPolling));

    if (result == null) {
      throw new AssertionError("Did not receive success in " + duration.getStandardSeconds() + "s");
    }

    return new String(result.get(0).elementBytes, UTF_8);
  }

  private void sleep(long t) {
    try {
      Thread.sleep(t);
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
  }

  /** {@link PTransform} that for validates whether elements seen so far match success criteria. */
  static class PublishSuccessWhen<T> extends PTransform<PCollection<? extends T>, POutput> {
    private Coder<T> coder;
    private SerializableFunction<Set<T>, Boolean> successPredicate;
    private String resultTopicPath;

    PublishSuccessWhen(
        Coder<T> coder,
        SerializableFunction<Set<T>, Boolean> successPredicate,
        String resultTopicPath) {

      this.coder = coder;
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
              ParDo.of(new StatefulPredicateCheck<>(coder, successPredicate)))
          // signal the success/failure to the result topic
          .apply("publishSuccess", PubsubIO.writeStrings().to(resultTopicPath));
    }
  }

  /**
   * Stateful {@link DoFn} which caches the elements it sees and checks whether they satisfy the
   * predicate.
   *
   * <p>When predicate is satisfied outputs "SUCCESS". If predicate throws execption, outputs
   * "FAILURE".
   */
  static class StatefulPredicateCheck<T> extends DoFn<KV<String, ? extends T>, String> {
    private SerializableFunction<Set<T>, Boolean> successPredicate;
    // keep all events seen so far in the state cell

    @StateId("seenEvents")
    private final StateSpec<SetState<T>> seenEvents;

    StatefulPredicateCheck(Coder<T> coder, SerializableFunction<Set<T>, Boolean> successPredicate) {
      this.seenEvents = StateSpecs.set(coder);
      this.successPredicate = successPredicate;
    }

    @ProcessElement
    public void processElement(
        ProcessContext context, @StateId("seenEvents") SetState<T> seenEvents) {

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
