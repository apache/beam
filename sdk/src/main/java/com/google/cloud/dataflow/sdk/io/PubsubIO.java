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

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.cloud.dataflow.sdk.values.PInput;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

/**
 * Read and Write {@link PTransform}s for Pub/Sub streams. These transforms create
 * and consume unbounded {@link com.google.cloud.dataflow.sdk.values.PCollection}s.
 *
 * <p> {@code PubsubIO}  is only usable
 * with the {@link com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner}
 * and requires
 * {@link com.google.cloud.dataflow.sdk.options.StreamingOptions#setStreaming(boolean)}
 * to be enabled.
 */
public class PubsubIO {
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
      Pattern.compile("/subscriptions/([^/]+)/(.+)");

  private static final Pattern TOPIC_REGEXP =
      Pattern.compile("/topics/([^/]+)/(.+)");

  private static final Pattern PUBSUB_NAME_REGEXP =
      Pattern.compile("[a-z][-._a-z0-9]+[a-z0-9]");

  private static final int PUBSUB_NAME_MAX_LENGTH = 255;

  private static final String SUBSCRIPTION_RANDOM_TEST_PREFIX = "_random/";
  private static final String SUBSCRIPTION_STARTING_SIGNAL = "_starting_signal/";
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
      if (subscription.startsWith(SUBSCRIPTION_RANDOM_TEST_PREFIX)
          || subscription.startsWith(SUBSCRIPTION_STARTING_SIGNAL)) {
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
   * A {@link PTransform} that continuously reads from a Pubsub stream and
   * returns a {@code PCollection<String>} containing the items from
   * the stream.
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
     * <p> The {#dropLateData} field allows you to control what to do with late data. This relaxes
     * the semantics of {@code GroupByKey}; see
     * {@link com.google.cloud.dataflow.sdk.transforms.GroupByKey} for additional information on
     * late data and windowing.
     *
     * <p> Note that the system can guarantee that no late data will ever be seen when it assigns
     * timestamps by arrival time (i.e. {@code timestampLabel} is not provided).
     */
    public static Bound<String> timestampLabel(String timestampLabel) {
      return new Bound<>(DEFAULT_PUBSUB_CODER).timestampLabel(timestampLabel);
    }

    /**
     * If true, then late-arriving data from this source will be dropped.
     *
     * <p> If late data is not dropped, data for a window can arrive after that window has already
     * been closed.  This relaxes the semantics of {@code GroupByKey}; see
     * {@link com.google.cloud.dataflow.sdk.transforms.GroupByKey}
     * for additional information on late data and windowing.
     */
    public static Bound<String> dropLateData(boolean dropLateData) {
      return new Bound<>(DEFAULT_PUBSUB_CODER).dropLateData(dropLateData);
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
     * A {@link PTransform} that reads from a PubSub source and returns
     * a unbounded PCollection containing the items from the stream.
     */
    @SuppressWarnings("serial")
    public static class Bound<T> extends PTransform<PInput, PCollection<T>> {
      /** The Pubsub topic to read from. */
      String topic;
      /** The Pubsub subscription to read from. */
      String subscription;
      /** The Pubsub label to read timestamps from. */
      String timestampLabel;
      /** If true, late data will be dropped. */
      Boolean dropLateData;
      /** The Pubsub label to read ids from. */
      String idLabel;
      /** The coder used to decode each record. */
      @Nullable
      final Coder<T> coder;

      Bound(Coder<T> coder) {
        this.dropLateData = true;
        this.coder = coder;
      }

      Bound(String name, String subscription, String topic, String timestampLabel,
          boolean dropLateData, Coder<T> coder, String idLabel) {
        super(name);
        if (subscription != null) {
          Validator.validateSubscriptionName(subscription);
        }
        if (topic != null) {
          Validator.validateTopicName(topic);
        }
        this.subscription = subscription;
        this.topic = topic;
        this.timestampLabel = timestampLabel;
        this.dropLateData = dropLateData;
        this.coder = coder;
        this.idLabel = idLabel;
      }

      /**
       * Returns a new PubsubIO.Read PTransform that's like this one but with the given
       * step name. Does not modify the object.
       */
      public Bound<T> named(String name) {
        return new Bound<>(name, subscription, topic, timestampLabel, dropLateData,
            coder, idLabel);
      }

      /**
       * Returns a new PubsubIO.Read PTransform that's like this one but reading from the
       * given subscription. Does not modify the object.
       */
      public Bound<T> subscription(String subscription) {
        return new Bound<>(name, subscription, topic, timestampLabel, dropLateData, coder, idLabel);
      }

      /**
       * Returns a new PubsubIO.Read PTransform that's like this one but reading from the
       * give topic. Does not modify the object.
       */
      public Bound<T> topic(String topic) {
        return new Bound<>(name, subscription, topic, timestampLabel, dropLateData, coder, idLabel);
      }

      /**
       * Returns a new PubsubIO.Read PTransform that's like this one but reading timestamps
       * from the given PubSub label. Does not modify the object.
       */
      public Bound<T> timestampLabel(String timestampLabel) {
        return new Bound<>(name, subscription, topic, timestampLabel, dropLateData, coder, idLabel);
      }

      /**
       * Returns a new PubsubIO.Read PTransform that's like this one but with the specified
       * setting for dropLateData. Does not modify the object.
       */
      public Bound<T> dropLateData(boolean dropLateData) {
        return new Bound<>(name, subscription, topic, timestampLabel, dropLateData, coder, idLabel);
      }

      /**
       * Returns a new PubsubIO.Read PTransform that's like this one but reading unique ids
       * from the given PubSub label. Does not modify the object.
       */
      public Bound<T> idLabel(String idLabel) {
        return new Bound<>(name, subscription, topic, timestampLabel, dropLateData, coder, idLabel);
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
        return new Bound<>(name, subscription, topic, timestampLabel, dropLateData, coder, idLabel);
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
        return PCollection.<T>createPrimitiveOutputInternal(
                input.getPipeline(),
                WindowingStrategy.globalDefault())
            .setCoder(coder);
      }

      @Override
      protected Coder<T> getDefaultOutputCoder() {
        return coder;
      }

      @Override
      protected String getKindString() {
        return "PubsubIO.Read";
      }

      public String getTopic() {
        return topic;
      }

      public String getSubscription() {
        return subscription;
      }

      public String getTimestampLabel() {
        return timestampLabel;
      }

      public boolean getDropLateData() {
        return dropLateData;
      }

      public String getIdLabel() {
        return idLabel;
      }

      static {
        // TODO: Figure out how to make this work under
        // DirectPipelineRunner.
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
     * A {@link PTransform} that writes a unbounded {@code PCollection<String>}
     * to a PubSub stream.
     */
    @SuppressWarnings("serial")
    public static class Bound<T> extends PTransform<PCollection<T>, PDone> {
      /** The Pubsub topic to publish to. */
      String topic;
      String timestampLabel;
      String idLabel;
      final Coder<T> coder;

      Bound(Coder<T> coder) {
        this.coder = coder;
      }

      Bound(String name, String topic, String timestampLabel, String idLabel, Coder<T> coder) {
        super(name);
        if (topic != null) {
          Validator.validateTopicName(topic);
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
        return new Bound<>(name, topic, timestampLabel, idLabel, coder);
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
        return PDone.in(input.getPipeline());
      }

      @Override
      protected Coder<Void> getDefaultOutputCoder() {
        return VoidCoder.of();
      }

      @Override
      protected String getKindString() {
        return "PubsubIO.Write";
      }

      public String getTopic() {
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

      static {
        // TODO: Figure out how to make this work under
        // DirectPipelineRunner.
      }
    }
  }
}
