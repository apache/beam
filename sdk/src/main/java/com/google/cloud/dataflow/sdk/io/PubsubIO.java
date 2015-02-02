/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.io;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindows;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.cloud.dataflow.sdk.values.PInput;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * [Whitelisting Required] Read and Write transforms for Pub/Sub streams. These transforms create
 * and consume unbounded {@link com.google.cloud.dataflow.sdk.values.PCollection}s.
 *
 * <p> <b>Important:</b> PubsubIO is experimental. It is not supported by the
 * {@link com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner} and is only supported in the
 * {@link com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner} for users whitelisted in a
 * streaming early access program and who enable
 * {@link com.google.cloud.dataflow.sdk.options.StreamingOptions#setStreaming(boolean)}.
 *
 * <p> You should expect this class to change significantly in future versions of the SDK
 * or be removed entirely.
 */
public class PubsubIO {

  /**
   * Project IDs must contain 6-63 lowercase letters, digits, or dashes.
   * IDs must start with a letter and may not end with a dash.
   * This regex isn't exact - this allows for patterns that would be rejected by
   * the service, but this is sufficient for basic parsing of table references.
   */
  private static final Pattern PROJECT_ID_REGEXP =
      Pattern.compile("[a-z][-a-z0-9:.]{4,61}[a-z0-9]");

  private static final Pattern SUBSCRIPTION_REGEXP =
      Pattern.compile("/subscriptions/([^/]+)/(.+)");

  private static final Pattern TOPIC_REGEXP =
      Pattern.compile("/topics/([^/]+)/(.+)");

  private static final Pattern PUBSUB_NAME_REGEXP =
      Pattern.compile("[a-z][-._a-z0-9]+[a-z0-9]");

  private static final int PUBSUB_NAME_MAX_LENGTH = 255;

  private static final String SUBSCRIPTION_RANDOM_TEST_PREFIX = "_random/";
  private static final String TOPIC_DEV_NULL_TEST_NAME = "/topics/dev/null";

  /**
   * Utility class to validate topic and subscription names.
   */
  public static class Validator {
    public static void validateTopicName(String topic) {
      if (topic.equals(TOPIC_DEV_NULL_TEST_NAME)) {
        return;
      }
      Matcher match = TOPIC_REGEXP.matcher(topic);
      if (!match.matches()) {
        throw new IllegalArgumentException(
            "Pubsub topic is not in /topics/project_id/topic_name format: "
            + topic);
      }
      validateProjectName(match.group(1));
      validatePubsubName(match.group(2));
    }

    public static void validateSubscriptionName(String subscription) {
      if (subscription.startsWith(SUBSCRIPTION_RANDOM_TEST_PREFIX)) {
        return;
      }
      Matcher match = SUBSCRIPTION_REGEXP.matcher(subscription);
      if (!match.matches()) {
        throw new IllegalArgumentException(
            "Pubsub subscription is not in /subscriptions/project_id/subscription_name format: "
            + subscription);
      }
      validateProjectName(match.group(1));
      validatePubsubName(match.group(2));
    }

    private static void validateProjectName(String project) {
      Matcher match = PROJECT_ID_REGEXP.matcher(project);
      if (!match.matches()) {
        throw new IllegalArgumentException(
            "Illegal project name specified in Pubsub subscription: " + project);
      }
    }

    private static void validatePubsubName(String name) {
      if (name.length() > PUBSUB_NAME_MAX_LENGTH) {
        throw new IllegalArgumentException(
            "Pubsub object name is longer than 255 characters: " + name);
      }

      if (name.startsWith("goog")) {
        throw new IllegalArgumentException(
            "Pubsub object name cannot start with goog: " + name);
      }

      Matcher match = PUBSUB_NAME_REGEXP.matcher(name);
      if (!match.matches()) {
        throw new IllegalArgumentException(
            "Illegal Pubsub object name specified: " + name
            + " Please see Javadoc for naming rules.");
      }
    }
  }

  /**
   * A PTransform that continuously reads from a Pubsub stream and
   * returns a {@code PCollection<String>} containing the items from
   * the stream.
   */
  // TODO: Support non-String encodings.
  public static class Read {
    public static Bound named(String name) {
      return new Bound().named(name);
    }

    /**
     * Creates and returns a PubsubIO.Read PTransform for reading from
     * a Pubsub topic with the specified publisher topic. Format for
     * Cloud Pubsub topic names should be of the form
     * {@code /topics/<project>/<topic>}, where {@code <project>} is the name of
     * the publishing project. The {@code <topic>} component must comply with
     * the below requirements.
     *
     * <ul>
     * <li>Can only contain lowercase letters, numbers, dashes ('-'), underscores ('_') and periods
     * ('.').</li>
     * <li>Must be between 3 and 255 characters.</li>
     * <li>Must begin with a letter.</li>
     * <li>Must end with a letter or a number.</li>
     * <li>Cannot begin with 'goog' prefix.</li>
     * </ul>
     */
    public static Bound topic(String topic) {
      return new Bound().topic(topic);
    }

    /**
     * Creates and returns a PubsubIO.Read PTransform for reading from
     * a specific Pubsub subscription. Mutually exclusive with
     * PubsubIO.Read.topic().
     * Cloud Pubsub subscription names should be of the form
     * {@code /subscriptions/<project>/<<subscription>},
     * where {@code <project>} is the name of the project the subscription belongs to.
     * The {@code <subscription>} component must comply with the below requirements.
     *
     * <ul>
     * <li>Can only contain lowercase letters, numbers, dashes ('-'), underscores ('_') and periods
     * ('.').</li>
     * <li>Must be between 3 and 255 characters.</li>
     * <li>Must begin with a letter.</li>
     * <li>Must end with a letter or a number.</li>
     * <li>Cannot begin with 'goog' prefix.</li>
     * </ul>
     */
    public static Bound subscription(String subscription) {
      return new Bound().subscription(subscription);
    }

    /**
     * A PTransform that reads from a PubSub source and returns
     * a unbounded PCollection containing the items from the stream.
     */
    @SuppressWarnings("serial")
    public static class Bound
        extends PTransform<PInput, PCollection<String>> {
      /** The Pubsub topic to read from. */
      String topic;
      /** The Pubsub subscription to read from. */
      String subscription;

      Bound() {}

      Bound(String name, String subscription, String topic) {
        super(name);
        if (subscription != null) {
          Validator.validateSubscriptionName(subscription);
        }
        if (topic != null) {
          Validator.validateTopicName(topic);
        }
        this.subscription = subscription;
        this.topic = topic;
      }

      public Bound named(String name) {
        return new Bound(name, subscription, topic);
      }

      public Bound subscription(String subscription) {
        return new Bound(name, subscription, topic);
      }

      public Bound topic(String topic) {
        return new Bound(name, subscription, topic);
      }

      @Override
      public PCollection<String> apply(PInput input) {
        if (topic == null && subscription == null) {
          throw new IllegalStateException(
              "need to set either the topic or the subscription for "
              + "a PubsubIO.Read transform");
        }
        if (topic != null && subscription != null) {
          throw new IllegalStateException(
              "Can't set both the topic and the subscription for a "
              + "PubsubIO.Read transform");
        }
        return PCollection.<String>createPrimitiveOutputInternal(
            new GlobalWindows());
      }

      @Override
      protected Coder<String> getDefaultOutputCoder() {
        return StringUtf8Coder.of();
      }

      @Override
      protected String getKindString() { return "PubsubIO.Read"; }

      public String getTopic() {
        return topic;
      }

      public String getSubscription() {
        return subscription;
      }

      static {
        // TODO: Figure out how to make this work under
        // DirectPipelineRunner.
      }
    }
  }


  /////////////////////////////////////////////////////////////////////////////

  /**
   * A PTransform that continuously writes a
   * {@code PCollection<String>} to a Pubsub stream.
   */
  // TODO: Support non-String encodings.
  public static class Write {
    public static Bound named(String name) {
      return new Bound().named(name);
    }

    /** The topic to publish to.
     * Cloud Pubsub topic names should be {@code /topics/<project>/<topic>},
     * where {@code <project>} is the name of the publishing project.
     */
    public static Bound topic(String topic) {
      return new Bound().topic(topic);
    }

    /**
     * A PTransfrom that writes a unbounded {@code PCollection<String>}
     * to a PubSub stream.
     */
    @SuppressWarnings("serial")
    public static class Bound
        extends PTransform<PCollection<String>, PDone> {
      /** The Pubsub topic to publish to. */
      String topic;

      Bound() {}

      Bound(String name, String topic) {
        super(name);
        if (topic != null) {
          Validator.validateTopicName(topic);
          this.topic = topic;
        }
      }

      public Bound named(String name) {
        return new Bound(name, topic);
      }

      public Bound topic(String topic) {
        return new Bound(name, topic);
      }

      @Override
      public PDone apply(PCollection<String> input) {
        if (topic == null) {
          throw new IllegalStateException(
              "need to set the topic of a PubsubIO.Write transform");
        }
        return new PDone();
      }

      @Override
      protected Coder<Void> getDefaultOutputCoder() {
        return VoidCoder.of();
      }

      @Override
      protected String getKindString() { return "PubsubIO.Write"; }

      public String getTopic() {
        return topic;
      }

      static {
        // TODO: Figure out how to make this work under
        // DirectPipelineRunner.
      }
    }
  }
}
