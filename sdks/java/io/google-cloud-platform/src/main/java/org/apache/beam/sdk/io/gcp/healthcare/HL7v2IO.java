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
package org.apache.beam.sdk.io.gcp.healthcare;

import com.google.api.services.healthcare.v1alpha2.model.Message;
import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.text.ParseException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.datastore.AdaptiveThrottler;
import org.apache.beam.sdk.io.gcp.healthcare.HL7v2IO.Write.Result;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link HL7v2IO} provides an API for reading from and writing to <a
 * href="https://cloud.google.com/healthcare/docs/concepts/hl7v2">Google Cloud Healthcare HL7v2 API.
 * </a>
 *
 * <p>Read HL7v2 Messages are fetched from the HL7v2 store based on the {@link PCollection} of
 * message IDs {@link String}s as {@link HL7v2IO.Read.Result} where one can call {@link
 * Read.Result#getMessages()} to retrived a {@link PCollection} containing the successfully fetched
 * {@link Message}s and/or {@link Read.Result#getFailedReads()} to retrieve a {@link PCollection} of
 * {@link HealthcareIOError} containing the msgID that could not be fetched and the exception, this
 * can be used to write to the dead letter storage system of your choosing.
 *
 * <p>Unbounded Example:
 *
 * <pre>{@code
 * PipelineOptions options = ...;
 * Pipeline p = Pipeline.create(options);
 *
 * HL7v2IO.Read.Result readResult = p
 *   .apply(
 *     "Read HL7v2 notifications",
 *     PubSubIO.readStrings().fromTopic(options.getNotificationSubscription()))
 *   .apply(HL7v2IO.readAll());
 *
 * // Write errors to your favorite dead letter  queue (e.g. Pub/Sub, GCS, BigQuery)
 * readResult.getFailedReads().apply("WriteToDeadLetterQueue", ...);
 *
 *
 * // Go about your happy path transformations.
 * PCollection<Message> out = readResult.getMessages().apply("ProcessFetchedMessages", ...);
 *
 * // Write using the Message.Ingest method of the HL7v2 REST API.
 * out.apply(HL7v2IO.ingestMessages(options.getOutputHL7v2Store()));
 *
 * pipeline.run();
 *
 * }***
 * </pre>
 *
 * <p>Bounded Example:
 *
 * <pre>{@code
 * PipelineOptions options = ...;
 * Pipeline p = Pipeline.create(options);
 *
 * HL7v2IO.Read.Result readResult = p
 *   .apply(
 *       "List messages in HL7v2 store with filter",
 *       ListHL7v2MessageIDs(
 *           Collections.singletonList(options.getInputHL7v2Store()), option.getHL7v2Filter()))
 *   .apply(HL7v2IO.readAll());
 *
 * // Write errors to your favorite dead letter  queue (e.g. Pub/Sub, GCS, BigQuery)
 * readResult.getFailedReads().apply("WriteToDeadLetterQueue", ...);
 *
 *
 * // Go about your happy path transformations.
 * PCollection<Message> out = readResult.getMessages().apply("ProcessFetchedMessages", ...);
 *
 * // Write using the Message.Ingest method of the HL7v2 REST API.
 * out.apply(HL7v2IO.ingestMessages(options.getOutputHL7v2Store()));
 *
 * pipeline.run().waitUntilFinish();
 * }***
 * </pre>
 */
public class HL7v2IO {

  private static Write.Builder write(String hl7v2Store) {
    return new AutoValue_HL7v2IO_Write.Builder().setHL7v2Store(hl7v2Store);
  }

  public static Read readAll() {
    return new Read();
  }

  /**
   * Write with Messages.Ingest method. @see <a
   * href=https://cloud.google.com/healthcare/docs/reference/rest/v1beta1/projects.locations.datasets.hl7V2Stores.messages/ingest></a>
   *
   * @param hl7v2Store the hl 7 v 2 store
   * @return the write
   */
  public static Write ingestMessages(String hl7v2Store) {
    return write(hl7v2Store).setWriteMethod(Write.WriteMethod.INGEST).build();
  }

  // TODO add hyper links to this doc string.
  /**
   * The type Read that reads HL7v2 message contents given a PCollection of message IDs strings.
   *
   * <p>These could be sourced from any {@link PCollection} of {@link String}s but the most popular
   * patterns would be {@link PubsubIO#readStrings()} reading a subscription on an HL7v2 Store's
   * notification channel topic or using {@link ListHL7v2MessageIDs} to list HL7v2 message IDs with
   * an optional filter using
   * https://cloud.google.com/healthcare/docs/reference/rest/v1beta1/projects.locations.datasets.hl7V2Stores.messages/list.
   */
  public static class Read extends PTransform<PCollection<String>, Read.Result> {

    public Read() {}

    public static class Result implements POutput, PInput {
      PCollection<Message> messages;


      PCollection<HealthcareIOError<String>> failedReads;
      PCollectionTuple pct;

      public static Result of(PCollectionTuple pct) throws IllegalArgumentException {
        if (pct.getAll()
            .keySet()
            .containsAll((Collection<?>) TupleTagList.of(OUT).and(DEAD_LETTER))) {
          return new Result(pct);
        } else {
          throw new IllegalArgumentException(
              "The PCollection tuple must have the HL7v2IO.Read.OUT "
                  + "and HL7v2IO.Read.DEAD_LETTER tuple tags");
        }
      }

      private Result(PCollectionTuple pct) {
        this.pct = pct;
        this.messages = pct.get(OUT);
        this.failedReads =
            pct.get(DEAD_LETTER).setCoder(new HealthcareIOErrorCoder<>(StringUtf8Coder.of()));
      }

      public PCollection<HealthcareIOError<String>> getFailedReads() {
        return failedReads;
      }

      public PCollection<Message> getMessages() {
        return messages;
      }

      @Override
      public Pipeline getPipeline() {
        return this.pct.getPipeline();
      }

      @Override
      public Map<TupleTag<?>, PValue> expand() {
        return ImmutableMap.of(OUT, messages);
      }

      @Override
      public void finishSpecifyingOutput(
          String transformName, PInput input, PTransform<?, ?> transform) {}
    }

    /** The tag for the main output of HL7v2 Messages. */
    public static final TupleTag<Message> OUT = new TupleTag<Message>() {};
    /** The tag for the deadletter output of HL7v2 Messages. */
    public static final TupleTag<HealthcareIOError<String>> DEAD_LETTER =
        new TupleTag<HealthcareIOError<String>>() {};

    @Override
    public Result expand(PCollection<String> input) {
      return input.apply("Fetch HL7v2 messages", new FetchHL7v2Message());
    }

    /**
     * DoFn to fetch a message from an Google Cloud Healthcare HL7v2 store based on msgID
     *
     * <p>This DoFn consumes a {@link PCollection} of notifications {@link String}s from the HL7v2
     * store, and fetches the actual {@link Message} object based on the id in the notification and
     * will output a {@link PCollectionTuple} which contains the output and dead-letter {@link
     * PCollection}.
     *
     * <p>The {@link PCollectionTuple} output will contain the following {@link PCollection}:
     *
     * <ul>
     *   <li>{@link HL7v2IO.Read#OUT} - Contains all {@link PCollection} records successfully read
     *       from the HL7v2 store.
     *   <li>{@link HL7v2IO.Read#DEAD_LETTER} - Contains all {@link PCollection} of {@link
     *       HealthcareIOError<String>} message IDs which failed to be fetched from the HL7v2 store,
     *       with error message and stacktrace.
     * </ul>
     *
     * <p>Example:
     *
     * <pre>{@code
     * PipelineOptions options = ...;
     * Pipeline pipeline = Pipeline.create(options)
     *
     * PCollection<String> msgIDs = pipeline.apply(
     *    "ReadHL7v2Notifications",
     *    PubsubIO.readStrings().fromSubscription(options.getInputSubscription()));
     *
     * PCollectionTuple fetchResults = msgIDs.apply(
     *    "FetchHL7v2Messages",
     *    new FetchHL7v2Message;
     *
     * // Write errors to your favorite dead letter  queue (e.g. Pub/Sub, GCS, BigQuery)
     * fetchResults.get(PubsubNotificationToHL7v2Message.DEAD_LETTER)
     *    .apply("WriteToDeadLetterQueue", ...);
     *
     * PCollection<Message> fetchedMessages = fetchResults.get(PubsubNotificationToHL7v2Message.OUT)
     *    .apply("ExtractFetchedMessage",
     *    MapElements
     *        .into(TypeDescriptor.of(Message.class))
     *        .via(FailsafeElement::getPayload));
     *
     * // Go about your happy path transformations.
     * fetchedMessages.apply("ProcessFetchedMessages", ...)
     *
     * }****
     * </pre>
     */
    public static class FetchHL7v2Message extends PTransform<PCollection<String>, Result> {
      // TODO: this should migrate to use the batch API once available

      /** Instantiates a new Fetch HL7v2 message DoFn. */
      public FetchHL7v2Message() {}

      @Override
      public Result expand(PCollection<String> msgIds) {
        return new Result(
            msgIds.apply(
                ParDo.of(new FetchHL7v2Message.HL7v2MessageGetFn())
                    .withOutputTags(HL7v2IO.Read.OUT, TupleTagList.of(HL7v2IO.Read.DEAD_LETTER))));
      }

      /** DoFn for fetching messages from the HL7v2 store with error handling. */
      public static class HL7v2MessageGetFn extends DoFn<String, Message> {

        private Counter failedMessageGets =
            Metrics.counter(FetchHL7v2Message.HL7v2MessageGetFn.class, "failed-message-reads");
        private static final Logger LOG =
            LoggerFactory.getLogger(FetchHL7v2Message.HL7v2MessageGetFn.class);
        private final Counter throttledSeconds =
            Metrics.counter(
                FetchHL7v2Message.HL7v2MessageGetFn.class, "cumulativeThrottlingSeconds");
        private final Counter successfulHL7v2MessageGets =
            Metrics.counter(
                FetchHL7v2Message.HL7v2MessageGetFn.class, "successfulHL7v2MessageGets");
        private HealthcareApiClient client;
        private transient AdaptiveThrottler throttler;

        /** Instantiates a new Hl 7 v 2 message get fn. */
        HL7v2MessageGetFn() {}

        /**
         * Instantiate healthcare client.
         *
         * @throws IOException the io exception
         */
        @Setup
        public void instantiateHealthcareClient() throws IOException {
          this.client = new HttpHealthcareApiClient();
        }

        /** Start bundle. */
        @StartBundle
        public void startBundle() {
          if (throttler == null) {
            throttler = new AdaptiveThrottler(1200000, 10000, 1.25);
          }
        }

        /**
         * Process element.
         *
         * @param context the context
         */
        @ProcessElement
        public void processElement(ProcessContext context) {
          String msgId = context.element();
          try {
            context.output(fetchMessage(this.client, msgId));
          } catch (Exception e) {
            failedMessageGets.inc();
            LOG.warn(
                String.format(
                    "Error fetching HL7v2 message with ID %s writing to Dead Letter "
                        + "Queue. Cause: %s Stack Trace: %s",
                    msgId, e.getMessage(), Throwables.getStackTraceAsString(e)));
            context.output(HL7v2IO.Read.DEAD_LETTER, HealthcareIOError.of(msgId, e));
          }
        }

        private Message fetchMessage(HealthcareApiClient client, String msgId)
            throws IOException, ParseException, IllegalArgumentException, InterruptedException {
          final int throttleWaitSeconds = 5;
          long startTime = System.currentTimeMillis();
          Sleeper sleeper = Sleeper.DEFAULT;
          if (throttler.throttleRequest(startTime)) {
            LOG.info(String.format("Delaying request for %s due to previous failures.", msgId));
            this.throttledSeconds.inc(throttleWaitSeconds);
            sleeper.sleep(throttleWaitSeconds * 1000);
          }

          com.google.api.services.healthcare.v1alpha2.model.Message msg =
              client.getHL7v2Message(msgId);

          this.throttler.successfulRequest(startTime);
          if (msg == null) {
            throw new IOException(String.format("GET request for %s returned null", msgId));
          }
          this.successfulHL7v2MessageGets.inc();
          return msg;
        }
      }
    }
  }

  /** The type List HL7v2 message IDs. */
  public static class ListHL7v2MessageIDs extends PTransform<PBegin, PCollection<String>> {

    private final List<String> hl7v2Stores;
    private final String filter;

    /**
     * Instantiates a new List HL7v2 message IDs with filter.
     *
     * @param hl7v2Stores the HL7v2 stores
     * @param filter the filter
     */
    ListHL7v2MessageIDs(List<String> hl7v2Stores, String filter) {
      this.hl7v2Stores = hl7v2Stores;
      this.filter = filter;
    }

    /**
     * Instantiates a new List HL7v2 message IDs without filter.
     *
     * @param hl7v2Stores the HL7v2 stores
     */
    ListHL7v2MessageIDs(List<String> hl7v2Stores) {
      this.hl7v2Stores = hl7v2Stores;
      this.filter = null;
    }

    @Override
    public PCollection<String> expand(PBegin input) {
      return input.apply(Create.of(this.hl7v2Stores)).apply(ParDo.of(new ListHL7v2Fn(this.filter)));
    }
  }

  /** The type List HL7v2 fn. */
  static class ListHL7v2Fn extends DoFn<String, String> {

    private final String filter;
    private transient HealthcareApiClient client;

    /**
     * Instantiates a new List HL7v2 fn.
     *
     * @param filter the filter
     */
    ListHL7v2Fn(String filter) {
      this.filter = filter;
    }

    /**
     * Init client.
     *
     * @throws IOException the io exception
     */
    @Setup
    void initClient() throws IOException {
      this.client = new HttpHealthcareApiClient();
    }

    /**
     * List messages.
     *
     * @param context the context
     * @throws IOException the io exception
     */
    @ProcessElement
    void listMessages(ProcessContext context) throws IOException {
      String hl7v2Store = context.element();
      // Output all elements of all pages.
      this.client.getHL7v2MessageIDStream(hl7v2Store, this.filter).forEach(context::output);
    }
  }

  /** The type Write. */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<Message>, Result> {

    /** The tag for the successful writes to HL7v2 store`. */
    public static final TupleTag<HealthcareIOError<Message>> SUCCESS =
        new TupleTag<HealthcareIOError<Message>>() {};
    /** The tag for the failed writes to HL7v2 store`. */
    public static final TupleTag<HealthcareIOError<Message>> FAILED =
        new TupleTag<HealthcareIOError<Message>>() {};

    /**
     * Gets HL7v2 store.
     *
     * @return the HL7v2 store
     */
    abstract String getHL7v2Store();

    /**
     * Gets write method.
     *
     * @return the write method
     */
    abstract WriteMethod getWriteMethod();

    @Override
    public Result expand(PCollection<Message> messages) {
      return messages.apply(new WriteHL7v2(this.getHL7v2Store(), this.getWriteMethod()));
    }

    /** The enum Write method. */
    public enum WriteMethod {
      // TODO there is a batch import method on the road-map that we should add here once released.
      /**
       * Ingest write method. @see <a
       * href=https://cloud.google.com/healthcare/docs/reference/rest/v1beta1/projects.locations.datasets.hl7V2Stores.messages/ingest></a>
       */
      INGEST,
      /** Batch import write method. */
      BATCH_IMPORT
    }

    /** The type Builder. */
    @AutoValue.Builder
    abstract static class Builder {

      /**
       * Sets HL7v2 store.
       *
       * @param hl7v2Store the HL7v2 store
       * @return the HL7v2 store
       */
      abstract Builder setHL7v2Store(String hl7v2Store);

      /**
       * Sets write method.
       *
       * @param writeMethod the write method
       * @return the write method
       */
      abstract Builder setWriteMethod(WriteMethod writeMethod);

      /**
       * Build write.
       *
       * @return the write
       */
      abstract Write build();
    }

    public static class Result implements POutput {
      private final Pipeline pipeline;
      private final PCollection<HealthcareIOError<Message>> failedInsertsWithErr;

      /** Creates a {@link HL7v2IO.Write.Result} in the given {@link Pipeline}. */
      static Result in(Pipeline pipeline, PCollection<HealthcareIOError<Message>> failedInserts) {
        return new Result(pipeline, failedInserts);
      }

      public PCollection<HealthcareIOError<Message>> getFailedInsertsWithErr() {
        return this.failedInsertsWithErr;
      }

      @Override
      public Pipeline getPipeline() {
        return this.pipeline;
      }

      @Override
      public Map<TupleTag<?>, PValue> expand() {
        failedInsertsWithErr.setCoder(new HealthcareIOErrorCoder<>(new MessageCoder()));
        return ImmutableMap.of(FAILED, failedInsertsWithErr);
      }

      @Override
      public void finishSpecifyingOutput(
          String transformName, PInput input, PTransform<?, ?> transform) {}

      private Result(
          Pipeline pipeline, PCollection<HealthcareIOError<Message>> failedInsertsWithErr) {
        this.pipeline = pipeline;
        this.failedInsertsWithErr = failedInsertsWithErr;
      }
    }
  }

  /** The type Write hl 7 v 2. */
  static class WriteHL7v2 extends PTransform<PCollection<Message>, Result> {
    private final String hl7v2Store;
    private final Write.WriteMethod writeMethod;

    /**
     * Instantiates a new Write hl 7 v 2.
     *
     * @param hl7v2Store the hl 7 v 2 store
     * @param writeMethod the write method
     */
    WriteHL7v2(String hl7v2Store, Write.WriteMethod writeMethod) {
      this.hl7v2Store = hl7v2Store;
      this.writeMethod = writeMethod;
    }

    @Override
    public Result expand(PCollection<Message> input) {
      PCollection<HealthcareIOError<Message>> failedInserts =
          input
              .apply(ParDo.of(new WriteHL7v2Fn(hl7v2Store, writeMethod)))
              .setCoder(new HealthcareIOErrorCoder<>(new MessageCoder()));
      return Write.Result.in(input.getPipeline(), failedInserts);
    }

    /** The type Write hl 7 v 2 fn. */
    static class WriteHL7v2Fn extends DoFn<Message, HealthcareIOError<Message>> {
      // TODO when the healthcare API releases a bulk import method this should use that to improve
      // throughput.

      private Counter failedMessageWrites =
          Metrics.counter(WriteHL7v2Fn.class, "failed-hl7v2-message-writes");
      private final String hl7v2Store;
      private final Write.WriteMethod writeMethod;

      private static final Logger LOG = LoggerFactory.getLogger(WriteHL7v2.WriteHL7v2Fn.class);
      private transient HealthcareApiClient client;

      /**
       * Instantiates a new Write HL7v2 fn.
       *
       * @param hl7v2Store the HL7v2 store
       * @param writeMethod the write method
       */
      WriteHL7v2Fn(String hl7v2Store, Write.WriteMethod writeMethod) {
        this.hl7v2Store = hl7v2Store;
        this.writeMethod = writeMethod;
      }

      /**
       * Init client.
       *
       * @throws IOException the io exception
       */
      @Setup
      public void initClient() throws IOException {
        this.client = new HttpHealthcareApiClient();
      }

      /**
       * Write messages.
       *
       * @param context the context
       */
      @ProcessElement
      public void writeMessages(ProcessContext context) {
        Message msg = context.element();
        // TODO could insert some lineage hook here?
        switch (writeMethod) {
          case BATCH_IMPORT:
            throw new UnsupportedOperationException("The Batch import API is not avaiable yet");
          case INGEST:
          default:
            try {
              client.ingestHL7v2Message(hl7v2Store, msg);
            } catch (Exception e) {
              failedMessageWrites.inc();
              LOG.warn(
                  String.format(
                      "Failed to ingest message Error: %s Stacktrace: %s",
                      e.getMessage(), Throwables.getStackTraceAsString(e)));
              HealthcareIOError<Message> err = HealthcareIOError.of(msg, e);
              LOG.warn(String.format("%s %s", err.getErrorMessage(), err.getStackTrace()));
              context.output(err);
            }
        }
      }
    }
  }
}
