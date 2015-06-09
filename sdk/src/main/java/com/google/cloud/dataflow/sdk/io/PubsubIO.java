/*
 * Copyright (C) 2015 Google Inc.
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

import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.AcknowledgeRequest;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.api.services.pubsub.model.PullRequest;
import com.google.api.services.pubsub.model.PullResponse;
import com.google.api.services.pubsub.model.ReceivedMessage;
import com.google.api.services.pubsub.model.Subscription;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.StreamingOptions;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.Transport;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollection.IsBounded;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.cloud.dataflow.sdk.values.PInput;

import org.joda.time.Duration;
import org.joda.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

/**
 * Read and Write {@link PTransform}s for Pub/Sub streams. These transforms create
 * and consume unbounded {@link com.google.cloud.dataflow.sdk.values.PCollection}s.
 */
public class PubsubIO {
  private static final Logger LOG = LoggerFactory.getLogger(PubsubIO.class);
  public static final Coder<String> DEFAULT_PUBSUB_CODER = StringUtf8Coder.of();

  /**
   * Project IDs must contain 6-63 lowercase letters, digits, or dashes.
   * IDs must start with a letter and may not end with a dash.
   * This regex isn't exact - this allows for patterns that would be rejected by
   * the service, but this is sufficient for basic parsing of table references.
   */
  private static final Pattern PROJECT_ID_REGEXP =
      Pattern.compile("[a-z][-a-z0-9:.]{4,61}[a-z0-9]");

  private static final Pattern SUBSCRIPTION_REGEXP =
      Pattern.compile("projects/([^/]+)/subscriptions/(.+)");

  private static final Pattern TOPIC_REGEXP =
      Pattern.compile("projects/([^/]+)/topics/(.+)");

  private static final Pattern V1BETA1_SUBSCRIPTION_REGEXP =
      Pattern.compile("/subscriptions/([^/]+)/(.+)");

  private static final Pattern V1BETA1_TOPIC_REGEXP =
      Pattern.compile("/topics/([^/]+)/(.+)");

  private static final Pattern PUBSUB_NAME_REGEXP =
      Pattern.compile("[a-z][-._a-z0-9]+[a-z0-9]");

  private static final int PUBSUB_NAME_MAX_LENGTH = 255;

  private static final String SUBSCRIPTION_RANDOM_TEST_PREFIX = "_random/";
  private static final String SUBSCRIPTION_STARTING_SIGNAL = "_starting_signal/";
  private static final String TOPIC_DEV_NULL_TEST_NAME = "/topics/dev/null";

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

  /**
   * Class representing a Pubsub Subscription.
   */
  public static class PubsubSubscription implements Serializable {
    private static final long serialVersionUID = 0L;
    private enum Type { NORMAL, FAKE }

    private final Type type;
    private final String project;
    private final String subscription;

    private PubsubSubscription(Type type, String project, String subscription) {
      this.type = type;
      this.project = project;
      this.subscription = subscription;
    }

    public static PubsubSubscription fromPath(String path) {
      if (path.startsWith(SUBSCRIPTION_RANDOM_TEST_PREFIX)
          || path.startsWith(SUBSCRIPTION_STARTING_SIGNAL)) {
        return new PubsubSubscription(Type.FAKE, "", path);
      }

      String projectName, subscriptionName;

      Matcher v1beta1Match = V1BETA1_SUBSCRIPTION_REGEXP.matcher(path);
      if (v1beta1Match.matches()) {
        LOG.warn("Saw subscription in v1beta1 format. Subscriptions should be in the format "
            + "projects/<project_id>/subscriptions/<subscription_name>");
        projectName = v1beta1Match.group(1);
        subscriptionName = v1beta1Match.group(2);
      } else {
        Matcher match = SUBSCRIPTION_REGEXP.matcher(path);
        if (!match.matches()) {
          throw new IllegalArgumentException(
              "Pubsub subscription is not in "
              + "projects/<project_id>/subscriptions/<subscription_name> format: " + path);
        }
        projectName = match.group(1);
        subscriptionName = match.group(2);
      }

      validateProjectName(projectName);
      validatePubsubName(subscriptionName);
      return new PubsubSubscription(Type.NORMAL, projectName, subscriptionName);
    }

    public String asV1Beta1Path() {
      if (type == Type.NORMAL) {
        return "/subscriptions/" + project + "/" + subscription;
      } else {
        return subscription;
      }
    }

    public String asV1Beta2Path() {
      if (type == Type.NORMAL) {
        return "projects/" + project + "/subscriptions/" + subscription;
      } else {
        return subscription;
      }
    }
  }

  /**
   * Class representing a Pubsub Topic.
   */
  public static class PubsubTopic implements Serializable {
    private static final long serialVersionUID = 0L;
    private enum Type { NORMAL, FAKE }

    private final Type type;
    private final String project;
    private final String topic;

    public PubsubTopic(Type type, String project, String topic) {
      this.type = type;
      this.project = project;
      this.topic = topic;
    }

    public static PubsubTopic fromPath(String path) {
      if (path.equals(TOPIC_DEV_NULL_TEST_NAME)) {
        return new PubsubTopic(Type.FAKE, "", path);
      }

      String projectName, topicName;

      Matcher v1beta1Match = V1BETA1_TOPIC_REGEXP.matcher(path);
      if (v1beta1Match.matches()) {
        LOG.warn("Saw topic in v1beta1 format.  Topics should be in the format "
            + "projects/<project_id>/topics/<topic_name>");
        projectName = v1beta1Match.group(1);
        topicName = v1beta1Match.group(2);
      } else {
        Matcher match = TOPIC_REGEXP.matcher(path);
        if (!match.matches()) {
          throw new IllegalArgumentException(
              "Pubsub topic is not in projects/<project_id>/topics/<topic_name> format: "
              + path);
        }
        projectName = match.group(1);
        topicName = match.group(2);
      }

      validateProjectName(projectName);
      validatePubsubName(topicName);
      return new PubsubTopic(Type.NORMAL, projectName, topicName);
    }

    public String asV1Beta1Path() {
      if (type == Type.NORMAL) {
        return "/topics/" + project + "/" + topic;
      } else {
        return topic;
      }
    }

    public String asV1Beta2Path() {
      if (type == Type.NORMAL) {
        return "projects/" + project + "/topics/" + topic;
      } else {
        return topic;
      }
    }
  }

  /**
   * A {@link PTransform} that continuously reads from a Pubsub stream and
   * returns a {@code PCollection<String>} containing the items from
   * the stream.
   *
   * <p> When running with a runner that only supports bounded {@code PCollection}s
   * (such as DirectPipelineRunner or DataflowPipelineRunner without --streaming), only a
   * bounded portion of the input Pubsub stream can be processed.  As such, either
   * {@link Bound#maxNumRecords} or {@link Bound#maxReadTime} must be set.
   */
  public static class Read {
    public static Bound<String> named(String name) {
      return new Bound<>(DEFAULT_PUBSUB_CODER).named(name);
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
     *
     * <p> Dataflow will start reading data published on this topic from the time the pipeline is
     * started. Any data published on the topic before the pipeline is started will not be read by
     * Dataflow.
     */
    public static Bound<String> topic(String topic) {
      return new Bound<>(DEFAULT_PUBSUB_CODER).topic(topic);
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
    public static Bound<String> subscription(String subscription) {
      return new Bound<>(DEFAULT_PUBSUB_CODER).subscription(subscription);
    }

    /**
     * Creates and returns a PubsubIO.Read PTransform where record timestamps are expected
     * to be provided using the PubSub labeling API. The {@code <timestampLabel>} parameter
     * specifies the label name. The label value sent to PubsSub is a numerical value representing
     * the number of milliseconds since the Unix epoch. For example, if using the joda time classes,
     * org.joda.time.Instant.getMillis() returns the correct value for this label.
     *
     * <p> If {@code <timestampLabel>} is not provided, the system will generate record timestamps
     * the first time it sees each record. All windowing will be done relative to these timestamps.
     *
     * <p> By default windows are emitted based on an estimate of when this source is likely
     * done producing data for a given timestamp (referred to as the Watermark; see
     * {@link com.google.cloud.dataflow.sdk.transforms.windowing.AfterWatermark} for more details).
     * Any late data will be handled by the trigger specified with the windowing strategy -- by
     * default it will be output immediately.
     *
     * <p> Note that the system can guarantee that no late data will ever be seen when it assigns
     * timestamps by arrival time (i.e. {@code timestampLabel} is not provided).
     */
    public static Bound<String> timestampLabel(String timestampLabel) {
      return new Bound<>(DEFAULT_PUBSUB_CODER).timestampLabel(timestampLabel);
    }

    /**
     * Creates and returns a PubSubIO.Read PTransform where unique record identifiers are
     * expected to be provided using the PubSub labeling API. The {@code <idLabel>} parameter
     * specifies the label name. The label value sent to PubSub can be any string value that
     * uniquely identifies this record.
     *
     * <p> If idLabel is not provided, Dataflow cannot guarantee that no duplicate data will be
     * delivered on the PubSub stream. In this case,  deduplication of the stream will be
     * stricly best effort.
     */
    public static Bound<String> idLabel(String idLabel) {
      return new Bound<>(DEFAULT_PUBSUB_CODER).idLabel(idLabel);
    }

   /**
     * Creates and returns a PubsubIO.Read PTransform that uses the given
     * {@code Coder<T>} to decode PubSub record into a value of type {@code T}.
     *
     * <p> By default, uses {@link StringUtf8Coder}, which just
     * returns the text lines as Java strings.
     *
     * @param <T> the type of the decoded elements, and the elements
     * of the resulting PCollection.
     */
    public static <T> Bound<T> withCoder(Coder<T> coder) {
      return new Bound<>(coder);
    }

    /**
     * Sets the maximum number of records that will be read from Pubsub.
     *
     * <p> Either this or {@link #maxReadTime} must be set for use as a bounded
     * {@code PCollection}.
     */
    public static Bound<String> maxNumRecords(int maxNumRecords) {
      return new Bound<>(DEFAULT_PUBSUB_CODER).maxNumRecords(maxNumRecords);
    }

    /**
     * Sets the maximum duration during which records will be read from Pubsub.
     *
     * <p> Either this or {@link #maxNumRecords} must be set for use as a bounded
     * {@code PCollection}.
     */
    public static Bound<String> maxReadTime(Duration maxReadTime) {
      return new Bound<>(DEFAULT_PUBSUB_CODER).maxReadTime(maxReadTime);
    }

    /**
     * A {@link PTransform} that reads from a PubSub source and returns
     * a unbounded PCollection containing the items from the stream.
     */
    @SuppressWarnings("serial")
    public static class Bound<T> extends PTransform<PInput, PCollection<T>> {
      /** The Pubsub topic to read from. */
      PubsubTopic topic;
      /** The Pubsub subscription to read from. */
      PubsubSubscription subscription;
      /** The Pubsub label to read timestamps from. */
      String timestampLabel;
      /** The Pubsub label to read ids from. */
      String idLabel;
      /** The coder used to decode each record. */
      @Nullable
      final Coder<T> coder;
      /** Stop after reading this many records. */
      int maxNumRecords;
      /** Stop after reading for this much time. */
      Duration maxReadTime;

      Bound(Coder<T> coder) {
        this.coder = coder;
      }

      Bound(String name, PubsubSubscription subscription, PubsubTopic topic, String timestampLabel,
          Coder<T> coder, String idLabel,
          int maxNumRecords, Duration maxReadTime) {
        super(name);
        if (subscription != null) {
          this.subscription = subscription;
        }
        if (topic != null) {
          this.topic = topic;
        }
        this.timestampLabel = timestampLabel;
        this.coder = coder;
        this.idLabel = idLabel;
        this.maxNumRecords = maxNumRecords;
        this.maxReadTime = maxReadTime;
      }

      /**
       * Returns a new PubsubIO.Read PTransform that's like this one but with the given
       * step name. Does not modify the object.
       */
      public Bound<T> named(String name) {
        return new Bound<>(name, subscription, topic, timestampLabel,
            coder, idLabel, maxNumRecords, maxReadTime);
      }

      /**
       * Returns a new PubsubIO.Read PTransform that's like this one but reading from the
       * given subscription. Does not modify the object.
       *
       * <p> Multiple readers reading from the same subscription will each receive
       * some arbirary portion of the data.  Most likely, separate readers should
       * use their own subscriptions.
       */
      public Bound<T> subscription(String subscription) {
        return new Bound<>(name, PubsubSubscription.fromPath(subscription), topic, timestampLabel,
            coder, idLabel, maxNumRecords, maxReadTime);
      }

      /**
       * Returns a new PubsubIO.Read PTransform that's like this one but reading from the
       * give topic. Does not modify the object.
       */
      public Bound<T> topic(String topic) {
        return new Bound<>(name, subscription, PubsubTopic.fromPath(topic), timestampLabel,
            coder, idLabel, maxNumRecords, maxReadTime);
      }

      /**
       * Returns a new PubsubIO.Read PTransform that's like this one but reading timestamps
       * from the given PubSub label. Does not modify the object.
       */
      public Bound<T> timestampLabel(String timestampLabel) {
        return new Bound<>(name, subscription, topic, timestampLabel, coder, idLabel,
            maxNumRecords, maxReadTime);
      }

      /**
       * Returns a new PubsubIO.Read PTransform that's like this one but reading unique ids
       * from the given PubSub label. Does not modify the object.
       */
      public Bound<T> idLabel(String idLabel) {
        return new Bound<>(name, subscription, topic, timestampLabel, coder, idLabel,
            maxNumRecords, maxReadTime);
      }

      /**
       * Returns a new PubsubIO.Read PTransform that's like this one but that uses the given
       * {@code Coder<X>} to decode each record into a value of type {@code X}.  Does not modify
       * this object.
       *
       * @param <X> the type of the decoded elements, and the
       * elements of the resulting PCollection.
       */
      public <X> Bound<X> withCoder(Coder<X> coder) {
        return new Bound<>(name, subscription, topic, timestampLabel, coder, idLabel,
            maxNumRecords, maxReadTime);
      }

      /**
       * Sets the maximum number of records that will be read from Pubsub.
       *
       * <p> Setting either this or {@link #maxNumRecords} will cause the output {@code PCollection}
       * to be bounded.
       */
      public Bound<T> maxNumRecords(int maxNumRecords) {
        return new Bound<>(name, subscription, topic, timestampLabel,
            coder, idLabel, maxNumRecords, maxReadTime);
      }

      /**
       * Sets the maximum duration during which records will be read from Pubsub.
       *
       * <p> Setting either this or {@link #maxNumRecords} will cause the output {@code PCollection}
       * to be bounded.
       */
      public Bound<T> maxReadTime(Duration maxReadTime) {
        return new Bound<>(name, subscription, topic, timestampLabel,
            coder, idLabel, maxNumRecords, maxReadTime);
      }

      @Override
      public PCollection<T> apply(PInput input) {
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

        boolean boundedOutput = getMaxNumRecords() > 0 || getMaxReadTime() != null;

        if (boundedOutput) {
          return input.getPipeline().begin()
              .apply(Create.of((Void) null)).setCoder(VoidCoder.of())
              .apply(ParDo.of(new PubsubReader()))
              .setCoder(coder);
        } else {
          return PCollection.<T>createPrimitiveOutputInternal(
                  input.getPipeline(), WindowingStrategy.globalDefault(), IsBounded.UNBOUNDED)
              .setCoder(coder);
        }
      }

      @Override
      protected Coder<T> getDefaultOutputCoder() {
        return coder;
      }

      @Override
      protected String getKindString() {
        return "PubsubIO.Read";
      }

      public PubsubTopic getTopic() {
        return topic;
      }

      public PubsubSubscription getSubscription() {
        return subscription;
      }

      public String getTimestampLabel() {
        return timestampLabel;
      }

      public Coder<T> getCoder() {
        return coder;
      }

      public String getIdLabel() {
        return idLabel;
      }

      public int getMaxNumRecords() {
        return maxNumRecords;
      }

      public Duration getMaxReadTime() {
        return maxReadTime;
      }

      private class PubsubReader extends DoFn<Void, T> {
        private static final long serialVersionUID = 0L;

        @Override
        public void processElement(ProcessContext c) throws IOException {
          Pubsub pubsubClient =
              Transport.newPubsubClient(c.getPipelineOptions().as(DataflowPipelineOptions.class))
                  .build();

          String subscription = getSubscription().asV1Beta2Path();
          if (subscription == null) {
            String topic = getTopic().asV1Beta2Path();
            String[] split = topic.split("/");
            subscription = "projects/" + split[1] + "/subscriptions/" + split[3]
                + "_dataflow_" + new Random().nextLong();
            Subscription subInfo = new Subscription()
                .setAckDeadlineSeconds(60)
                .setTopic(topic);
            try {
              pubsubClient.projects().subscriptions().create(subscription, subInfo).execute();
            } catch (Exception e) {
              throw new RuntimeException("Failed to create subscription: ", e);
            }
          }

          Instant endTime = getMaxReadTime() == null
              ? new Instant(Long.MAX_VALUE) : Instant.now().plus(getMaxReadTime());

          List<PubsubMessage> messages = new ArrayList<>();

          try {
            while ((getMaxNumRecords() == 0 || messages.size() < getMaxNumRecords())
                && Instant.now().isBefore(endTime)) {
              PullRequest pullRequest = new PullRequest().setReturnImmediately(false);
              if (getMaxNumRecords() > 0) {
                pullRequest.setMaxMessages(getMaxNumRecords() - messages.size());
              }

              PullResponse pullResponse =
                  pubsubClient.projects().subscriptions().pull(subscription, pullRequest).execute();
              List<String> ackIds = new ArrayList<>();
              for (ReceivedMessage received : pullResponse.getReceivedMessages()) {
                messages.add(received.getMessage());
                ackIds.add(received.getAckId());
              }

              if (ackIds.size() != 0) {
                AcknowledgeRequest ackRequest = new AcknowledgeRequest().setAckIds(ackIds);
                pubsubClient.projects()
                    .subscriptions()
                    .acknowledge(subscription, ackRequest)
                    .execute();
              }
            }
          } catch (IOException e) {
            throw new RuntimeException("Unexected exception while reading from Pubsub: ", e);
          } finally {
            if (getTopic() != null) {
              try {
                pubsubClient.projects().subscriptions().delete(subscription).execute();
              } catch (IOException e) {
                throw new RuntimeException("Failed to delete subscription: ", e);
              }
            }
          }

          for (PubsubMessage message : messages) {
            Instant timestamp;
            if (getTimestampLabel() == null) {
              timestamp = Instant.now();
            } else {
              if (message.getAttributes() == null
                  || !message.getAttributes().containsKey(getTimestampLabel())) {
                throw new RuntimeException(
                    "Message from pubsub missing timestamp label: " + getTimestampLabel());
              }
              timestamp = new Instant(Long.parseLong(
                      message.getAttributes().get(getTimestampLabel())));
            }
            c.outputWithTimestamp(
                CoderUtils.decodeFromByteArray(getCoder(), message.decodeData()),
                timestamp);
          }
        }
      }
    }
  }


  /////////////////////////////////////////////////////////////////////////////

  /**
   * A {@link PTransform} that continuously writes a
   * {@code PCollection<String>} to a Pubsub stream.
   */
  // TODO: Support non-String encodings.
  public static class Write {
    public static Bound<String> named(String name) {
      return new Bound<>(DEFAULT_PUBSUB_CODER).named(name);
    }

    /** The topic to publish to.
     * Cloud Pubsub topic names should be {@code /topics/<project>/<topic>},
     * where {@code <project>} is the name of the publishing project.
     */
    public static Bound<String> topic(String topic) {
      return new Bound<>(DEFAULT_PUBSUB_CODER).topic(topic);
    }

    /**
     * If specified, Dataflow will add a Pubsub label to each output record specifying the logical
     * timestamp of the record. {@code <timestampLabel>} determines the label name. The label value
     * is a numerical value representing the number of milliseconds since the Unix epoch. For
     * example, if using the joda time classes, the org.joda.time.Instant(long) constructor can be
     * used to parse this value. If the output from this sink is being read by another Dataflow
     * source, then PubsubIO.Read.timestampLabel can be used to ensure that the other source reads
     * these timestamps from the appropriate label.
     */
    public static Bound<String> timestampLabel(String timestampLabel) {
      return new Bound<>(DEFAULT_PUBSUB_CODER).timestampLabel(timestampLabel);
    }

    /**
     * If specified, Dataflow will add a Pubsub label to each output record containing a unique
     * identifier for that record. {@code <idLabel>} determines the label name. The label value
     * is an opaque string value. This is useful if the the output from this sink is being read
     * by another Dataflow source, in which case PubsubIO.Read.idLabel can be used to ensure that
     * the other source reads these ids from the appropriate label.
     */
    public static Bound<String> idLabel(String idLabel) {
      return new Bound<>(DEFAULT_PUBSUB_CODER).idLabel(idLabel);
    }

   /**
     * Returns a TextIO.Write PTransform that uses the given
     * {@code Coder<T>} to encode each of the elements of the input
     * {@code PCollection<T>} into an output PubSub record.
     *
     * <p> By default, uses {@link StringUtf8Coder}, which writes input
     * Java strings directly as records.
     *
     * @param <T> the type of the elements of the input PCollection
     */
    public static <T> Bound<T> withCoder(Coder<T> coder) {
      return new Bound<>(coder);
    }

    /**
     * A {@link PTransform} that writes an unbounded {@code PCollection<String>}
     * to a PubSub stream.
     */
    @SuppressWarnings("serial")
    public static class Bound<T> extends PTransform<PCollection<T>, PDone> {
      /** The Pubsub topic to publish to. */
      PubsubTopic topic;
      String timestampLabel;
      String idLabel;
      final Coder<T> coder;

      Bound(Coder<T> coder) {
        this.coder = coder;
      }

      Bound(String name, PubsubTopic topic, String timestampLabel, String idLabel, Coder<T> coder) {
        super(name);
        if (topic != null) {
          this.topic = topic;
        }
        this.timestampLabel = timestampLabel;
        this.idLabel = idLabel;
        this.coder = coder;
      }

      /**
       * Returns a new PubsubIO.Write PTransform that's like this one but with the given step
       * name. Does not modify the object.
       */
      public Bound<T> named(String name) {
        return new Bound<>(name, topic, timestampLabel, idLabel, coder);
      }

      /**
       * Returns a new PubsubIO.Write PTransform that's like this one but writing to the given
       * topic. Does not modify the object.
       */
      public Bound<T> topic(String topic) {
        return new Bound<>(name, PubsubTopic.fromPath(topic), timestampLabel, idLabel, coder);
      }

      /**
       * Returns a new PubsubIO.Write PTransform that's like this one but publishing timestamps
       * to the given PubSub label. Does not modify the object.
       */
      public Bound<T> timestampLabel(String timestampLabel) {
        return new Bound<>(name, topic, timestampLabel, idLabel, coder);
      }

      /**
       * Returns a new PubsubIO.Write PTransform that's like this one but publishing record ids
       * to the given PubSub label. Does not modify the object.
       */
     public Bound<T> idLabel(String idLabel) {
       return new Bound<>(name, topic, timestampLabel, idLabel, coder);
      }

     /**
       * Returns a new PubsubIO.Write PTransform that's like this one
       * but that uses the given {@code Coder<X>} to encode each of
       * the elements of the input {@code PCollection<X>} into an
       * output record.  Does not modify this object.
       *
       * @param <X> the type of the elements of the input PCollection
       */
      public <X> Bound<X> withCoder(Coder<X> coder) {
        return new Bound<>(name, topic, timestampLabel, idLabel, coder);
      }

      @Override
      public PDone apply(PCollection<T> input) {
        if (topic == null) {
          throw new IllegalStateException(
              "need to set the topic of a PubsubIO.Write transform");
        }

        if (input.getPipeline().getOptions().as(StreamingOptions.class).isStreaming()) {
          return PDone.in(input.getPipeline());
        } else {
          input.apply(ParDo.of(new PubsubWriter()));
          return PDone.in(input.getPipeline());
        }
      }

      @Override
      protected Coder<Void> getDefaultOutputCoder() {
        return VoidCoder.of();
      }

      @Override
      protected String getKindString() {
        return "PubsubIO.Write";
      }

      public PubsubTopic getTopic() {
        return topic;
      }

      public String getTimestampLabel() {
        return timestampLabel;
      }

      public String getIdLabel() {
        return idLabel;
      }

      public Coder<T> getCoder() {
        return coder;
      }

      private class PubsubWriter extends DoFn<T, Void> {
        private static final long serialVersionUID = 0L;
        private static final int MAX_PUBLISH_BATCH_SIZE = 100;
        private transient List<PubsubMessage> output;
        private transient Pubsub pubsubClient;


        @Override
        public void startBundle(Context c) {
          this.output = new ArrayList<>();
          this.pubsubClient =
              Transport.newPubsubClient(c.getPipelineOptions().as(DataflowPipelineOptions.class))
                  .build();
        }

        @Override
        public void processElement(ProcessContext c) throws IOException {
          PubsubMessage message = new PubsubMessage().encodeData(
              CoderUtils.encodeToByteArray(getCoder(), c.element()));
          if (getTimestampLabel() != null) {
            Map<String, String> attributes = message.getAttributes();
            if (attributes == null) {
              attributes = new HashMap<>();
              message.setAttributes(attributes);
            }
            attributes.put(
                getTimestampLabel(), String.valueOf(c.timestamp().getMillis()));
          }
          output.add(message);

          if (output.size() >= MAX_PUBLISH_BATCH_SIZE) {
            publish();
          }
        }

        @Override
        public void finishBundle(Context c) throws IOException {
          if (!output.isEmpty()) {
            publish();
          }
        }

        private void publish() throws IOException {
          PublishRequest publishRequest = new PublishRequest().setMessages(output);
          pubsubClient.projects().topics()
              .publish(getTopic().asV1Beta2Path(), publishRequest).execute();
          output.clear();
        }
      }
    }
  }
}
