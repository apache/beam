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
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;

/**
 * {@link HL7v2IO} provides an API for reading from and writing to <a
 * href="https://cloud.google.com/healthcare/docs/concepts/hl7v2">Google Cloud Healthcare HL7v2 API.
 * </a>
 *
 * <p>Read HL7v2 Messages are fetched from the HL7v2 store based on the {@link PCollection<String>}
 * of message IDs produced by the {@link AutoValue_HL7v2IO_Read#getMessageIDTransform} as {@link
 * PCollectionTuple}** containing an {@link FetchHL7v2Message#OUT} tag for successfully fetched
 * messages and a {@link FetchHL7v2Message#DEAD_LETTER} tag for message IDs that could not be
 * fetched.
 *
 * <p>HL7v2 stores can be read in several ways: - Unbounded: based on the Pub/Sub Notification
 * Channel {@link HL7v2IO#readNotificationSubscription(String)} - Bounded: based on reading an
 * entire HL7v2 store (or stores) {@link HL7v2IO#readHL7v2Store(String)} - Bounded: based on reading
 * an HL7v2 store with a filter
 *
 * <p>Example:
 *
 * <pre>{@code
 * PipelineOptions options = ...;
 * Pipeline pipeline = Pipeline.create(options)
 *
 *
 * PCollectionTuple messages = pipeline.apply(
 *     new HLv2IO.readNotifications(options.getNotificationSubscription())
 *
 * // Write errors to your favorite dead letter  queue (e.g. Pub/Sub, GCS, BigQuery)
 * messages.get(PubsubNotificationToHL7v2Message.DEAD_LETTER)
 *    .apply("WriteToDeadLetterQueue", ...);
 *
 * PCollection<Message> fetchedMessages = fetchResults.get(PubsubNotificationToHL7v2Message.OUT)
 *    .apply("ExtractFetchedMessage",
 *    MapElements
 *        .into(TypeDescriptor.of(Message.class))
 *        .via(FailsafeElement::getPayload));
 *
 * // Go about your happy path transformations.
 * PCollection<Message> out = fetchedMessages.apply("ProcessFetchedMessages", ...);
 *
 * // Write using the Message.Ingest method of the HL7v2 REST API.
 * out.apply(HL7v2IO.ingestMessages(options.getOutputHL7v2Store()));
 *
 * pipeline.run();
 *
 * }**
 * </pre>
 */
public class HL7v2IO {

  private static Read.Builder read(PTransform<PBegin, PCollection<String>> messageIDTransform) {
    Read.Builder builder = new AutoValue_HL7v2IO_Read.Builder();
    return builder.setMessageIDTransform(messageIDTransform);
  }

  /**
   * Read the HL7v2 message IDs from a Pub/Sub Subscription to the notification channel.
   *
   * @see <a
   *     href=https://cloud.google.com/healthcare/docs/concepts/pubsub#hl7v2_message_data_and_attributes></a>
   * @param subscription the subscription
   * @return the read
   */
  public static Read readNotificationSubscription(String subscription) {
    return read(PubsubIO.readStrings().fromSubscription(subscription)).build();
  }

  /**
   * Read an entire HL7v2 store.
   *
   * @param hl7v2Store the HL7v2 store
   * @return the read
   */
  public static Read readHL7v2Store(String hl7v2Store) {
    return read(new ListHL7v2MessageIDs(Collections.singletonList(hl7v2Store))).build();
  }

  /**
   * Read HL7v2 store read.
   *
   * @param hl7v2Store the HL7v2 store
   * @param filter the filter
   * @return the read
   */
  public static Read readHL7v2Store(String hl7v2Store, String filter) {
    return read(new ListHL7v2MessageIDs(Collections.singletonList(hl7v2Store), filter)).build();
  }

  /**
   * Read several entire HL7v2 stores.
   *
   * @param hl7v2Stores the HL7v2 stores
   * @return the read
   */
  public static Read readHL7v2Stores(List<String> hl7v2Stores) {
    return read(new ListHL7v2MessageIDs(hl7v2Stores)).build();
  }

  /**
   * Read HL7v2 stores read.
   *
   * @param hl7v2Stores the HL7v2 stores
   * @param filter the filter
   * @return the read
   */
  public static Read readHL7v2Stores(List<String> hl7v2Stores, String filter) {
    return read(new ListHL7v2MessageIDs(hl7v2Stores, filter)).build();
  }

  private static Write.Builder write(String hl7v2Store) {
    return new AutoValue_HL7v2IO_Write.Builder().setHL7v2Store(hl7v2Store);
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

  /** The type Read. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollectionTuple> {

    /**
     * Gets message id transform.
     *
     * @return the message id transform
     */
    abstract PTransform<PBegin, PCollection<String>> getMessageIDTransform();

    @Override
    public PCollectionTuple expand(PBegin input) {
      return input
          .apply("Get Message IDs", this.getMessageIDTransform())
          .apply("Fetch HL7v2 messages", new FetchHL7v2Message());
    }

    /** The type Builder. */
    @AutoValue.Builder
    abstract static class Builder {

      /**
       * Sets message id transform.
       *
       * @param messageIDTransform the message id transform
       * @return the message id transform
       */
      abstract Builder setMessageIDTransform(
          PTransform<PBegin, PCollection<String>> messageIDTransform);

      /**
       * Build read.
       *
       * @return the read
       */
      abstract Read build();
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
  public abstract static class Write extends PTransform<PCollection<Message>, PDone> {

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
    public PDone expand(PCollection<Message> messages) {
      messages.apply(ParDo.of(new WriteHL7v2Fn(this.getHL7v2Store(), this.getWriteMethod())));
      return PDone.in(messages.getPipeline());
    }

    /** The enum Write method. */
    public enum WriteMethod {
      // TODO there is a batch import method on the road-map that we should add here once released.
      /**
       * Ingest write method. @see <a
       * href=https://cloud.google.com/healthcare/docs/reference/rest/v1beta1/projects.locations.datasets.hl7V2Stores.messages/ingest></a>
       */
      INGEST,
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
  }

  /** The type Write HL7v2 fn. */
  static class WriteHL7v2Fn extends DoFn<Message, PDone> {
    // TODO when the healthcare API releases a bulk import method this should use that to improve
    // throughput.

    /** The HL7v2 store. */
    private final String hl7v2Store;
    /** The Write method. */
    private final Write.WriteMethod writeMethod;

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
    void initClient() throws IOException {
      this.client = new HttpHealthcareApiClient();
    }

    /**
     * Write messages.
     *
     * @param context the context
     * @throws IOException the io exception
     */
    @ProcessElement
    void writeMessages(ProcessContext context) throws IOException {
      Message msg = context.element();
      // TODO could insert some lineage hook here?
      // TODO add case for Batch import API method.
      switch (writeMethod) {
        case INGEST:
        default:
          client.ingestHL7v2Message(hl7v2Store, msg);
      }
    }
  }
}
