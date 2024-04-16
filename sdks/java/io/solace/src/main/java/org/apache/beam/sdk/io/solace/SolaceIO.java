/*
 * Copyright 2023 Google.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.dataflow.dce.io.solace;

import com.google.auto.value.AutoValue;
import com.google.cloud.dataflow.dce.io.solace.broker.SempClientFactory;
import com.google.cloud.dataflow.dce.io.solace.broker.SessionService;
import com.google.cloud.dataflow.dce.io.solace.broker.SessionServiceFactory;
import com.google.cloud.dataflow.dce.io.solace.data.Solace;
import com.google.cloud.dataflow.dce.io.solace.data.Solace.SolaceRecordMapper;
import com.google.cloud.dataflow.dce.io.solace.data.SolaceRecordCoder;
import com.google.cloud.dataflow.dce.io.solace.read.UnboundedSolaceSource;
import com.google.cloud.dataflow.dce.io.solace.write.SolacePublishResult;
import com.google.cloud.dataflow.dce.io.solace.write.UnboundedBatchedSolaceWriter;
import com.google.cloud.dataflow.dce.io.solace.write.UnboundedSolaceWriter;
import com.google.cloud.dataflow.dce.io.solace.write.UnboundedStreamingSolaceWriter;
import com.google.cloud.dataflow.dce.io.solace.write.properties.BasicAuthenticationProvider;
import com.google.cloud.dataflow.dce.io.solace.write.properties.GoogleCloudSecretProvider;
import com.google.cloud.dataflow.dce.io.solace.write.properties.SessionPropertiesProvider;
import com.google.common.base.Preconditions;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.Topic;
import java.io.IOException;
import java.util.Objects;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PTransform} to read and write from/to Solace.
 *
 * <h3>Authentication</h3>
 *
 * TODO: Auth for the read connector
 *
 * <p>The writer connector uses a JCSMP session properties provider, where all the authentication
 * details must be set. See {@link Write#withSessionPropertiesProvider(SessionPropertiesProvider)}.
 * For convenience, the connector provides a provider for basic authentication ({@link
 * BasicAuthenticationProvider}) and another one to retrieve the password from Google Cloud Secret
 * Manager ({@link GoogleCloudSecretProvider}
 *
 * <h3>Reading</h3>
 *
 * TODO
 *
 * <h3>Writing</h3>
 *
 * To write to Solace, use {@link #write()}. The default VPN name is "default".
 *
 * <h4>Coders and schemas support
 *
 * <p>The connector expects a {@link Solace.Record} as input, so you need to transform your data to
 * this format before writing. {@link Solace.Record} uses Beam schemas by default, but you can
 * override and set {@link SolaceRecordCoder} as custom coder if you prefer.
 *
 * <h4>Writing to a topic of a queue
 *
 * <p>The connector uses the <a href=
 * "https://docs.solace.com/API/Messaging-APIs/JCSMP-API/jcsmp-api-home.htm">Solace JCSMP API</a>.
 * The clients will write to a <a href="https://docs.solace.com/Messaging/SMF-Topics.htm">SMF
 * topic</a> to the port 55555 of the host. If you want to use a different port, specify it in the
 * host property with the format "X.X.X.X:PORT".
 *
 * <p>Once you have a {@link PCollection} of {@link Solace.Record}, you can write to Solace using
 *
 * <pre>{@code
 * PCollection<Solace.Record> solaceRecs = ...;
 *
 * PCollection<Solace.PublishResult> results =
 *         solaceRecs.apply(
 *                 "Write to Solace",
 *                 SolaceIO.write()
 *                         .to(SolaceIO.topicFromName("some-topic"))
 *                         .withSessionProvider(
 *                            BasicAuthenticationProvider.builder()
 *                              .username("username")
 *                              .password("password")
 *                              .host("host:port")
 *                              .build()));
 * }</pre>
 *
 * The above code snippet will write to the VPN named "default", using 4 clients per worker (VM in
 * Dataflow), and a maximum of 20 workers/VMs for writing (default values). You can change the
 * default VPN name by setting the required JCSMP property in the session properties provider (in
 * this case, with {@link BasicAuthenticationProvider#vpnName()}), the number of clients per worker
 * with {@link Write#withNumberOfClientsPerWorker(int)} and the number of parallel write clients
 * using {@link Write#withMaxNumOfUsedWorkers(int)}.
 *
 * <h4>Direct and persistent messages, and latency metrics</h4>
 *
 * <p>The connector can write either direct or persistent messages. The default mode is DIRECT.
 *
 * <p>The connector returns a {@link PCollection} of {@link Solace.PublishResult}, that you can use
 * to get a confirmation of messages that have been published, or rejected, but only if it is
 * publishing persistent messages.
 *
 * <p>If you are publishing persistent messages, then you can have some feedback about whether the
 * messages have been published, and some publishing latency metrics. If the message has been
 * published, {@link Solace.PublishResult#getPublished()} will be true. If it is false, it means
 * that the message could not be published, and {@link Solace.PublishResult#getError()} will contain
 * more details about why the message could not be published. To get latency metrics as well as the
 * results, set the property {@link Write#publishLatencyMetrics()}.
 *
 * <h4>Throughput and latency
 *
 * <p>This connector can work in two main modes: high latency or high throughput. The default mode
 * favors high throughput over high latency. You can control this behavior with the methods {@link
 * Write#withSubmissionMode(SubmissionMode)} and {@link Write#withWriterType(WriterType)}.
 *
 * <p>The default mode works like the following options:
 *
 * <pre>{@code
 * PCollection<Solace.Record> solaceRecs = ...;
 *
 * PCollection<Solace.PublishResult> results =
 *         solaceRecs.apply(
 *                 "Write to Solace",
 *                 SolaceIO.write()
 *                         .to(SolaceIO.topicFromName("some-topic"))
 *                         .withSessionProvider(
 *                            BasicAuthenticationProvider.builder()
 *                              .username("username")
 *                              .password("password")
 *                              .host("host:port")
 *                              .build())
 *                         .withSubmissionMode(SubmissionMode.HIGHER_THROUGHPUT)
 *                         .withWriterType(WriterType.BATCHED));
 * }</pre>
 *
 * <p>{@link SubmissionMode#HIGHER_THROUGHPUT} and {@link WriterType#BATCHED} are the default
 * values, and offer the higher possible throughput, and the lowest usage of resources in the runner
 * side (due to the lower backpressure).
 *
 * <p>This connector writes bundles of 50 messages, using a bulk publish JCSMP method. This will
 * increase the latency, since a message needs to "wait" until 50 messages are accumulated, before
 * they are submitted to Solace.
 *
 * <p>For the lowest latency possible, use {@link SubmissionMode#LOWER_LATENCY} and {@link
 * WriterType#STREAMING}.
 *
 * <pre>{@code
 * PCollection<Solace.PublishResult> results =
 *         solaceRecs.apply(
 *                 "Write to Solace",
 *                 SolaceIO.write()
 *                         .to(SolaceIO.topicFromName("some-topic"))
 *                         .withSessionProvider(
 *                            BasicAuthenticationProvider.builder()
 *                              .username("username")
 *                              .password("password")
 *                              .host("host:port")
 *                              .build())
 *                         .withSubmissionMode(SubmissionMode.LOWER_LATENCY)
 *                         .withWriterType(WriterType.STREAMING));
 * }</pre>
 *
 * The streaming connector publishes each message individually, without holding up or batching
 * before the message is sent to Solace. This will ensure the lowest possible latency, but it will
 * offer a much lower throughput. The streaming connector does not use state & timers.
 *
 * <p>Both connectors uses state & timers to control the level of parallelism. If you are using
 * Cloud Dataflow, it is recommended that you enable <a
 * href="https://cloud.google.com/dataflow/docs/streaming-engine">Streaming Engine</a> to use this
 * connector.
 *
 * <h3>Connector retries
 *
 * <p>When the worker using the connector is created, the connector will attempt to connect to
 * Solace.
 *
 * <p>If the client cannot connect to Solace for whatever reason, the connector will retry the
 * connections using the following strategy. There will be a maximum of 4 retries. The first retry
 * will be attempted 1 second after the first connection attempt. Every subsequent retry will
 * multiply that time by a factor of two, with a maximum of 10 seconds.
 *
 * <p>If after those retries the client is still unable to connect to Solace, the connector will
 * attempt to reconnect using the same strategy repeated for every single incoming message. If for
 * some reason, there is a persistent issue that prevents the connection (e.g. client quota
 * exhausted), you will need to stop your job manually, or the connector will keep retrying.
 *
 * <p>This strategy is applied to all the remote calls sent to Solace, either to connect, pull
 * messages, push messages, etc.
 */
public class SolaceIO {

    public static final Logger LOG = LoggerFactory.getLogger(SolaceIO.class);
    public static final SerializableFunction<Solace.Record, Instant> SENDER_TIMESTAMP_FUNCTION =
            (record) -> new Instant(record.getSenderTimestamp());
    public static final int DEFAULT_MAX_NUMBER_OF_WORKERS = 20;
    public static final int DEFAULT_CLIENTS_PER_WORKER = 4;
    public static final Boolean DEFAULT_PUBLISH_LATENCY_METRICS = false;
    public static final SubmissionMode DEFAULT_SUBMISSION_MODE = SubmissionMode.HIGHER_THROUGHPUT;
    public static final DeliveryMode DEFAULT_DELIVERY_MODE = DeliveryMode.DIRECT;
    public static final WriterType DEFAULT_WRITER_TYPE = WriterType.BATCHED;
    private static final boolean DEFAULT_DEDUPLICATE_RECORDS = true;

    public enum SubmissionMode {
        HIGHER_THROUGHPUT,
        LOWER_LATENCY
    }

    public enum WriterType {
        STREAMING,
        BATCHED
    }

    /** Get a {@link Topic} object from the topic name. */
    static Topic topicFromName(String topicName) {
        return JCSMPFactory.onlyInstance().createTopic(topicName);
    }

    /** Get a {@link Queue} object from the queue name. */
    static Queue queueFromName(String queueName) {
        return JCSMPFactory.onlyInstance().createQueue(queueName);
    }

    /**
     * Convert to a JCSMP destination from a schema-enabled {@link
     * com.google.cloud.dataflow.dce.io.solace.data.Solace.Destination}.
     *
     * <p>This method returns a {@link Destination}, which may be either a {@link Topic} or a {@link
     * Queue}
     */
    public static Destination convertToJcsmpDestination(Solace.Destination destination) {
        if (destination.getType().equals(Solace.DestinationType.TOPIC)) {
            return topicFromName(destination.getName());
        } else if (destination.getType().equals(Solace.DestinationType.QUEUE)) {
            return queueFromName(destination.getName());
        } else {
            throw new IllegalArgumentException(
                    "SolaceIO.Write: Unknown destination type: " + destination.getType());
        }
    }

    /** Create a {@link Write} transform, to write to Solace with a custom type. */
    public static <T> Write<T> write() {
        return Write.<T>builder().build();
    }

    /** Create a {@link Write} transform, to write to Solace using {@link Solace.Record} objects. */
    public static Write<Solace.Record> writeSolaceRecords() {
        return Write.<Solace.Record>builder().build();
    }

    /**
     * Create a {@link Read} transform, to read from Solace. The ingested records will be mapped to
     * the {@link Solace.Record} objects.
     */
    public static Read<Solace.Record> read() {
        return Read.<Solace.Record>builder()
                .setTypeDescriptor(TypeDescriptor.of(Solace.Record.class))
                .setParseFn(SolaceRecordMapper::map)
                .setTimestampFn(SENDER_TIMESTAMP_FUNCTION)
                .build();
    }
    /**
     * Create a {@link Read} transform, to read from Solace. Specify a {@link SerializableFunction}
     * to map incoming {@link BytesXMLMessage} records, to the object of your choice. You also need
     * to specify a {@link TypeDescriptor} for your class and the timestamp function which returns
     * an {@link Instant} from the record.
     *
     * <p> The type descriptor will be used to infer a coder from CoderRegistry or Schema Registry.
     * You can initialize a new TypeDescriptor in the following manner:
     *
     * <pre>{@code
     *   TypeDescriptor<T> typeDescriptor = TypeDescriptor.of(YourOutputType.class);
     * }
     */
    public static <T> Read<T> read(
            TypeDescriptor<T> typeDescriptor,
            SerializableFunction<BytesXMLMessage, T> parseFn,
            SerializableFunction<T, Instant> timestampFn) {
        return Read.<T>builder()
                .setTypeDescriptor(typeDescriptor)
                .setParseFn(parseFn)
                .setTimestampFn(timestampFn)
                .build();
    }

    @AutoValue
    public abstract static class Write<T> extends PTransform<PCollection<T>, SolacePublishResult> {

        private static final Logger LOG = LoggerFactory.getLogger(Write.class);

        public static final TupleTag<Solace.PublishResult> FAILED_PUBLISH_TAG = new TupleTag<>() {};
        public static final TupleTag<Solace.PublishResult> SUCCESSFUL_PUBLISH_TAG =
                new TupleTag<>() {};

        /**
         * Write to a Solace topic.
         *
         * <p>The topic does not need to exist before launching the pipeline.
         *
         * <p>This will write all records to the same topic, ignoring their destination field.
         *
         * <p>Optional. If not specified, the connector will use dynamic destinations based on the
         * destination field of {@link Solace.Record}.
         */
        public Write<T> to(Solace.Topic topic) {
            return toBuilder().setDestination(topicFromName(topic.getName())).build();
        }

        /**
         * Write to a Solace queue.
         *
         * <p>The queue must exist prior to launching the pipeline.
         *
         * <p>This will write all records to the same queue, ignoring their destination field.
         *
         * <p>Optional. If not specified, the connector will use dynamic destinations based on the
         * destination field of {@link Solace.Record}.
         */
        public Write<T> to(Solace.Queue queue) {
            return toBuilder().setDestination(queueFromName(queue.getName())).build();
        }

        /**
         * The number of workers used by the job to write to Solace.
         *
         * <p>This is optional, the default value is 20.
         *
         * <p>This is the maximum value that the job would use, but depending on the amount of data,
         * the actual number of writers may be lower than this value. With the Dataflow runner, the
         * connector will as maximum this number of VMs in the job (but the job itself may use more
         * VMs).
         *
         * <p>Set this number taking into account the limits in the number of clients in your Solace
         * cluster, and the need for performance when writing to Solace (more workers will achieve
         * higher throughput).
         */
        public Write<T> withMaxNumOfUsedWorkers(int maxNumOfUsedWorkers) {
            return toBuilder().setMaxNumOfUsedWorkers(maxNumOfUsedWorkers).build();
        }

        /**
         * The number of clients that each worker will create.
         *
         * <p>This is optional, the default number is 4.
         *
         * <p>The number of clients is per worker. If there are more than one worker, the number of
         * clients will be multiplied by the number of workers. With the Dataflow runner, this will
         * be the number of clients created per VM. The clients will be re-used across different
         * threads in the same worker.
         *
         * <p>Set this number in combination with {@link #withMaxNumOfUsedWorkers}, to ensure that
         * the limit for number of clients in your Solace cluster is not exceeded.
         *
         * <p>Normally, using a higher number of clients with fewer workers will achieve better
         * throughput at a lower cost, since the workers are better utilized. A good rule of thumb
         * to use is setting as many clients per worker as vCPUs the worker has.
         */
        public Write<T> withNumberOfClientsPerWorker(int numberOfClientsPerWorker) {
            return toBuilder().setNumberOfClientsPerWorker(numberOfClientsPerWorker).build();
        }

        /**
         * Set the delivery mode. This is optional, the default value is DIRECT.
         *
         * <p>For more details, see <a
         * href="https://docs.solace.com/API/API-Developer-Guide/Message-Delivery-Modes.htm">https://docs.solace.com/API/API-Developer-Guide/Message-Delivery-Modes.htm</a>
         */
        public Write<T> withDeliveryMode(DeliveryMode deliveryMode) {
            return toBuilder().setDeliveryMode(deliveryMode).build();
        }

        /**
         * Publish latency metrics using Beam metrics.
         *
         * <p>Latency metrics are only available if {@link #withDeliveryMode(DeliveryMode)} is set
         * to PERSISTENT. In that mode, latency is measured for each single message, as the time
         * difference between the message creation and the reception of the publishing confirmation.
         *
         * <p>For the batched writer, the creation time is set for every message in a batch shortly
         * before the batch is submitted. So the latency is very close to the actual publishing
         * latency, and it does not take into account the time spent waiting for the batch to be
         * submitted.
         *
         * <p>This is optional, the default value is false (don't publish latency metrics).
         */
        public Write<T> publishLatencyMetrics() {
            return toBuilder().setPublishLatencyMetrics(true).build();
        }

        /**
         * This setting controls the JCSMP property MESSAGE_CALLBACK_ON_REACTOR. Optional.
         *
         * <p>For full details, please check <a
         * href="https://docs.solace.com/API/API-Developer-Guide/Java-API-Best-Practices.htm">https://docs.solace.com/API/API-Developer-Guide/Java-API-Best-Practices.htm</a>.
         *
         * <p>The Solace JCSMP client libraries can dispatch messages using two different modes:
         *
         * <p>One of the modes dispatches messages directly from the same thread that is doing the
         * rest of I/O work. This mode favors lower latency but lower throughput. Set this to
         * LOWER_LATENCY to use that mode (MESSAGE_CALLBACK_ON_REACTOR set to True).
         *
         * <p>The other mode uses a parallel thread to accumulate and dispatch messages. This mode
         * favors higher throughput but also has higher latency. Set this to HIGHER_THROUGHPUT to
         * use that mode. This is the default mode (MESSAGE_CALLBACK_ON_REACTOR set to False).
         *
         * <p>This is optional, the default value is HIGHER_THROUGHPUT.
         */
        public Write<T> withSubmissionMode(SubmissionMode submissionMode) {
            return toBuilder().setDispatchMode(submissionMode).build();
        }

        /**
         * Set the type of writer used by the connector. Optional.
         *
         * <p>The Solace writer can either use the JCSMP modes in streaming or batched.
         *
         * <p>In streaming mode, the publishing latency will be lower, but the throughput will also
         * be lower.
         *
         * <p>With the batched mode, messages are accumulated until a batch size of 50 is reached,
         * or 5 seconds have elapsed since the first message in the batch was received. The 50
         * messages are sent to Solace in a single batch. This writer offers higher throughput but
         * higher publishing latency, as messages can be held up for up to 5 seconds until they are
         * published.
         *
         * <p>Notice that this is the message publishing latency, not the end-to-end latency. For
         * very large scale pipelines, you will probably prefer to use the HIGHER_THROUGHPUT mode,
         * as with lower throughput messages will accumulate in the pipeline, and the end-to-end
         * latency may actually be higher.
         *
         * <p>This is optional, the default is the BATCHED writer.
         */
        public Write<T> withWriterType(WriterType writerType) {
            return toBuilder().setWriterType(writerType).build();
        }

        /**
         * Set the format function for your custom data type, and/or for dynamic destinations.
         *
         * <p>If you are using a custom data class, this function should return a {@link
         * Solace.Record} corresponding to your custom data class instance.
         *
         * <p>If you are using this formatting function with dynamic destinations, you must ensure
         * that you set the right value in the destination value of the {@link Solace.Record}
         * messages.
         *
         * <p>In any other case, this format function is optional.
         */
        public Write<T> withFormatFunction(SerializableFunction<T, Solace.Record> formatFunction) {
            return toBuilder().setFormatFunction(formatFunction).build();
        }

        /**
         * Set the provider used to obtain the properties to initialize a new session in the broker.
         *
         * <p>This provider should define the destination host where the broker is listening, and
         * all the properties related to authentication (base auth, client certificate, etc.).
         */
        public Write<T> withSessionPropertiesProvider(SessionPropertiesProvider provider) {
            return toBuilder().setSessionPropertiesProvider(provider).build();
        }

        abstract int getMaxNumOfUsedWorkers();

        abstract int getNumberOfClientsPerWorker();

        abstract @Nullable Destination getDestination();

        abstract DeliveryMode getDeliveryMode();

        abstract boolean getPublishLatencyMetrics();

        abstract SubmissionMode getDispatchMode();

        abstract WriterType getWriterType();

        abstract @Nullable SerializableFunction<T, Solace.Record> getFormatFunction();

        abstract @Nullable SessionPropertiesProvider getSessionPropertiesProvider();

        static <T> Builder<T> builder() {
            return new AutoValue_SolaceIO_Write.Builder<T>()
                    .setDeliveryMode(DEFAULT_DELIVERY_MODE)
                    .setMaxNumOfUsedWorkers(DEFAULT_MAX_NUMBER_OF_WORKERS)
                    .setNumberOfClientsPerWorker(DEFAULT_CLIENTS_PER_WORKER)
                    .setPublishLatencyMetrics(DEFAULT_PUBLISH_LATENCY_METRICS)
                    .setDispatchMode(DEFAULT_SUBMISSION_MODE)
                    .setWriterType(DEFAULT_WRITER_TYPE);
        }

        abstract Builder<T> toBuilder();

        @AutoValue.Builder
        abstract static class Builder<T> {
            abstract Builder<T> setMaxNumOfUsedWorkers(int maxNumOfUsedWorkers);

            abstract Builder<T> setNumberOfClientsPerWorker(int numberOfClientsPerWorker);

            abstract Builder<T> setDestination(Destination topicOrQueue);

            abstract Builder<T> setDeliveryMode(DeliveryMode deliveryMode);

            abstract Builder<T> setPublishLatencyMetrics(Boolean publishLatencyMetrics);

            abstract Builder<T> setDispatchMode(SubmissionMode submissionMode);

            abstract Builder<T> setWriterType(WriterType writerType);

            abstract Builder<T> setFormatFunction(
                    SerializableFunction<T, Solace.Record> formatFunction);

            abstract Builder<T> setSessionPropertiesProvider(
                    SessionPropertiesProvider propertiesProvider);

            abstract Write<T> build();
        }

        @Override
        public SolacePublishResult expand(PCollection<T> input) {
            Class<? super T> pcollClass = input.getTypeDescriptor().getRawType();
            boolean usingSolaceRecord = pcollClass.isAssignableFrom(Solace.Record.class);
            // todo definitely don't merge. Seems like a bug!
            // usingSolaceRecord = true;
            // System.out.println(pcollClass + " " +
            // pcollClass.isAssignableFrom(Solace.Record.class) );
            validate(usingSolaceRecord);

            // Register schema for subtypes needed by Record

            boolean usingDynamicDestinations = getDestination() == null;
            SerializableFunction<Solace.Record, Destination> destinationFn;
            if (usingDynamicDestinations) {
                destinationFn = x -> SolaceIO.convertToJcsmpDestination(x.getDestination());
            } else {
                // Constant destination for all messages (same topic or queue)
                destinationFn = x -> getDestination();
            }

            @SuppressWarnings("unchecked")
            PCollection<Solace.Record> records =
                    getFormatFunction() == null
                            ? (PCollection<Solace.Record>) input
                            : input.apply(
                                    "Format records",
                                    MapElements.into(TypeDescriptor.of(Solace.Record.class))
                                            .via(getFormatFunction()));

            // Store the current window used by the input
            PCollection<Solace.PublishResult> captureWindow =
                    records.apply(
                            "Capture window",
                            ParDo.of(new UnboundedSolaceWriter.RecordToPublishResultDoFn()));

            @SuppressWarnings("unchecked")
            WindowingStrategy<Solace.PublishResult, BoundedWindow> windowingStrategy =
                    (WindowingStrategy<Solace.PublishResult, BoundedWindow>)
                            captureWindow.getWindowingStrategy();

            PCollection<KV<Integer, Solace.Record>> withShardKeys =
                    records.apply(
                            "Add shard key",
                            ParDo.of(
                                    new UnboundedSolaceWriter.AddShardKeyDoFn(
                                            getMaxNumOfUsedWorkers())));

            PCollection<KV<Integer, Solace.Record>> withGlobalWindow =
                    withShardKeys.apply("Global window", Window.into(new GlobalWindows()));

            String label =
                    getWriterType() == WriterType.STREAMING
                            ? "Publish (streaming)"
                            : "Publish (batched)";

            PCollectionTuple solaceOutput =
                    withGlobalWindow.apply(label, getWriterTransform(destinationFn));

            SolacePublishResult output;
            if (getDeliveryMode() == DeliveryMode.PERSISTENT) {
                PCollection<Solace.PublishResult> failedPublish =
                        solaceOutput.get(FAILED_PUBLISH_TAG);
                PCollection<Solace.PublishResult> successfulPublish =
                        solaceOutput.get(SUCCESSFUL_PUBLISH_TAG);
                output =
                        rewindow(
                                SolacePublishResult.in(
                                        input.getPipeline(), failedPublish, successfulPublish),
                                windowingStrategy);
            } else {
                LOG.info(
                        String.format(
                                "Solace.Write: omitting writer output because delivery mode is %s",
                                getDeliveryMode()));
                output = SolacePublishResult.in(input.getPipeline(), null, null);
            }

            return output;
        }

        private ParDo.MultiOutput<KV<Integer, Solace.Record>, Solace.PublishResult>
                getWriterTransform(SerializableFunction<Solace.Record, Destination> destinationFn) {

            ParDo.SingleOutput<KV<Integer, Solace.Record>, Solace.PublishResult> writer =
                    ParDo.of(
                            getWriterType() == WriterType.STREAMING
                                    ? new UnboundedStreamingSolaceWriter.WriterDoFn(
                                            destinationFn,
                                            getSessionPropertiesProvider(),
                                            getDeliveryMode(),
                                            getDispatchMode(),
                                            getNumberOfClientsPerWorker(),
                                            getPublishLatencyMetrics())
                                    : new UnboundedBatchedSolaceWriter.WriterDoFn(
                                            destinationFn,
                                            getSessionPropertiesProvider(),
                                            getDeliveryMode(),
                                            getDispatchMode(),
                                            getNumberOfClientsPerWorker(),
                                            getPublishLatencyMetrics()));

            return writer.withOutputTags(
                    FAILED_PUBLISH_TAG, TupleTagList.of(SUCCESSFUL_PUBLISH_TAG));
        }

        private SolacePublishResult rewindow(
                SolacePublishResult solacePublishResult,
                WindowingStrategy<Solace.PublishResult, BoundedWindow> strategy) {
            PCollection<Solace.PublishResult> correct = solacePublishResult.getSuccessfulPublish();
            PCollection<Solace.PublishResult> failed = solacePublishResult.getFailedPublish();

            PCollection<Solace.PublishResult> correctWithWindow = null;
            PCollection<Solace.PublishResult> failedWithWindow = null;

            if (correct != null) {
                correctWithWindow = applyOriginalWindow(correct, strategy, "Rewindow correct");
            }

            if (failed != null) {
                failedWithWindow = applyOriginalWindow(failed, strategy, "Rewindow failed");
            }

            return SolacePublishResult.in(
                    solacePublishResult.getPipeline(), failedWithWindow, correctWithWindow);
        }

        private static PCollection<Solace.PublishResult> applyOriginalWindow(
                PCollection<Solace.PublishResult> pcoll,
                WindowingStrategy<Solace.PublishResult, BoundedWindow> strategy,
                String label) {
            Window<Solace.PublishResult> originalWindow = captureWindowDetails(strategy);

            if (strategy.getMode() == WindowingStrategy.AccumulationMode.ACCUMULATING_FIRED_PANES) {
                originalWindow = originalWindow.accumulatingFiredPanes();
            } else {
                originalWindow = originalWindow.discardingFiredPanes();
            }

            return pcoll.apply(label, originalWindow);
        }

        private static Window<Solace.PublishResult> captureWindowDetails(
                WindowingStrategy<Solace.PublishResult, BoundedWindow> strategy) {
            return Window.<Solace.PublishResult>into(strategy.getWindowFn())
                    .withAllowedLateness(strategy.getAllowedLateness())
                    .withOnTimeBehavior(strategy.getOnTimeBehavior())
                    .withTimestampCombiner(strategy.getTimestampCombiner())
                    .triggering(strategy.getTrigger());
        }

        /**
         * Called before running the Pipeline to verify this transform is fully and correctly
         * specified.
         */
        private void validate(boolean usingSolaceRecords) {
            if (!usingSolaceRecords) {
                Preconditions.checkArgument(
                        getFormatFunction() != null,
                        "SolaceIO.Write: If you are not using Solace.Record as the input type, you"
                                + " must set a format function using withFormatFunction().");
            }

            Preconditions.checkArgument(
                    getMaxNumOfUsedWorkers() > 0,
                    "SolaceIO.Write: The number of used workers must be positive.");
            Preconditions.checkArgument(
                    getNumberOfClientsPerWorker() > 0,
                    "SolaceIO.Write: The number of clients per worker must be positive.");
            Preconditions.checkArgument(
                    getDeliveryMode() == DeliveryMode.DIRECT
                            || getDeliveryMode() == DeliveryMode.PERSISTENT,
                    String.format(
                            "SolaceIO.Write: Delivery mode must be either DIRECT or PERSISTENT. %s"
                                    + " not supported",
                            getDeliveryMode()));
            if (getPublishLatencyMetrics()) {
                Preconditions.checkArgument(
                        getDeliveryMode() == DeliveryMode.PERSISTENT,
                        "SolaceIO.Write: Publish latency metrics can only be enabled for PERSISTENT"
                                + " delivery mode.");
            }
            Preconditions.checkArgument(
                    getSessionPropertiesProvider() != null,
                    "SolaceIO: You need to pass a session properties provider. For basic"
                            + " authentication, you can use BasicAuthenticationProvider.");
        }
    }

    @AutoValue
    public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {
        private static final Logger LOG = LoggerFactory.getLogger(Read.class);

        /** Set the queue name to read from. Use this or the `from(Topic)` method. */
        public Read<T> from(Solace.Queue queue) {
            return toBuilder().setQueue(queueFromName(queue.getName())).build();
        }

        /** Set the topic name to read from. Use this or the `from(Queue)` method. */
        public Read<T> from(Solace.Topic topic) {
            return toBuilder().setTopic(topicFromName(topic.getName())).build();
        }

        /**
         * Set the timestamp function. This serializable has to output an {@link Instant}. This will
         * be used to calculate watermark and define record's timestamp.
         */
        public Read<T> withTimestampFn(SerializableFunction<T, Instant> timestampFn) {
            return toBuilder().setTimestampFn(timestampFn).build();
        }

        /**
         * Maximum number of read connections created to Solace cluster. This is optional, leave out
         * to let the Runner decide.
         */
        public Read<T> withMaxNumConnections(Integer maxNumConnections) {
            return toBuilder().setMaxNumConnections(maxNumConnections).build();
        }

        /**
         * Set if the read records should be deduplicated. True by default. It will use the
         * `applicationMessageId` attribute to identify duplicates.
         */
        public Read<T> withDeduplicateRecords(boolean deduplicateRecords) {
            return toBuilder().setDeduplicateRecords(deduplicateRecords).build();
        }

        /**
         * Set a factory that creates a {@link
         * com.google.cloud.dataflow.dce.io.solace.broker.BrokerService}.
         *
         * <p>The factory `create()` method is invoked in each instance of an {@link
         * com.google.cloud.dataflow.dce.io.solace.read.UnboundedSolaceReader}. Created {@link
         * com.google.cloud.dataflow.dce.io.solace.broker.BrokerService} has to communicate with
         * broker management API. It must support operations such as:
         *
         * <ul>
         *   <li>query for outstanding backlog bytes in a Queue,
         *   <li>query for metadata such as access-type of a Queue,
         *   <li>requesting creation of new Queues.
         * </ul>
         */
        public Read<T> withSempClientFactory(SempClientFactory sempClientFactory) {
            return toBuilder().setSempClientFactory(sempClientFactory).build();
        }

        /**
         * Set a factory that creates a {@link SessionService}.
         *
         * <p>The factory `create()` method is invoked in each instance of an {@link
         * com.google.cloud.dataflow.dce.io.solace.read.UnboundedSolaceReader}. Created {@link
         * SessionService} has to be able to:
         *
         * <ul>
         *   <li>initialize a connection with the broker,
         *   <li>check liveliness of the connection,
         *   <li>close the connection,
         *   <li>create a {@link com.google.cloud.dataflow.dce.io.solace.broker.MessageReceiver}.
         * </ul>
         */
        public Read<T> withSessionServiceFactory(SessionServiceFactory sessionServiceFactory) {
            return toBuilder().setSessionServiceFactory(sessionServiceFactory).build();
        }

        abstract @Nullable Queue getQueue();

        abstract @Nullable Topic getTopic();

        abstract @Nullable SerializableFunction<T, Instant> getTimestampFn();

        abstract @Nullable Integer getMaxNumConnections();

        abstract boolean getDeduplicateRecords();

        abstract SerializableFunction<BytesXMLMessage, T> getParseFn();

        abstract @Nullable SempClientFactory getSempClientFactory();

        abstract @Nullable SessionServiceFactory getSessionServiceFactory();

        abstract TypeDescriptor<T> getTypeDescriptor();

        public static <T> Builder<T> builder() {
            Builder<T> builder = new AutoValue_SolaceIO_Read.Builder<T>();
            builder.setDeduplicateRecords(DEFAULT_DEDUPLICATE_RECORDS);
            return builder;
        }

        abstract Builder<T> toBuilder();

        @AutoValue.Builder
        public abstract static class Builder<T> {

            abstract Builder<T> setQueue(Queue queue);

            abstract Builder<T> setTopic(Topic topic);

            abstract Builder<T> setTimestampFn(SerializableFunction<T, Instant> timestampFn);

            abstract Builder<T> setMaxNumConnections(Integer maxNumConnections);

            abstract Builder<T> setDeduplicateRecords(boolean deduplicateRecords);

            abstract Builder<T> setParseFn(SerializableFunction<BytesXMLMessage, T> parseFn);

            abstract Builder<T> setSempClientFactory(SempClientFactory brokerServiceFactory);

            abstract Builder<T> setSessionServiceFactory(
                    SessionServiceFactory sessionServiceFactory);

            abstract Builder<T> setTypeDescriptor(TypeDescriptor<T> typeDescriptor);

            abstract Read<T> build();
        }

        @Override
        public PCollection<T> expand(PBegin input) {
            validate();

            SempClientFactory sempClientFactory = getSempClientFactory();
            String jobName = input.getPipeline().getOptions().getJobName();
            Queue queue =
                    getQueue() != null
                            ? getQueue()
                            : initializeQueueForTopic(jobName, sempClientFactory);

            SessionServiceFactory sessionServiceFactory = getSessionServiceFactory();
            sessionServiceFactory.setQueue(queue);

            registerDefaultCoder(input.getPipeline());
            // Infer the actual coder
            Coder<T> coder = inferCoder(input.getPipeline());

            return input.apply(
                    org.apache.beam.sdk.io.Read.from(
                            new UnboundedSolaceSource<>(
                                    queue,
                                    sempClientFactory,
                                    sessionServiceFactory,
                                    getMaxNumConnections(),
                                    getDeduplicateRecords(),
                                    coder,
                                    getTimestampFn(),
                                    getParseFn())));
        }

        private static void registerDefaultCoder(Pipeline pipeline) {
            pipeline.getCoderRegistry()
                    .registerCoderForType(
                            TypeDescriptor.of(Solace.Record.class), SolaceRecordCoder.of());
        }

        @VisibleForTesting
        Coder<T> inferCoder(Pipeline pipeline) {
            Coder<T> coderFromCoderRegistry = getFromCoderRegistry(pipeline);
            if (coderFromCoderRegistry != null) {
                return coderFromCoderRegistry;
            }

            Coder<T> coderFromSchemaRegistry = getFromSchemaRegistry(pipeline);
            if (coderFromSchemaRegistry != null) {
                return coderFromSchemaRegistry;
            }

            throw new RuntimeException(
                    "SolaceIO.Read: Cannot infer a coder for the TypeDescriptor. Annotate you"
                        + " output class with @DefaultSchema annotation or create a coder manually"
                        + " and register it in the CoderRegistry.");
        }

        private Coder<T> getFromSchemaRegistry(Pipeline pipeline) {
            try {
                return pipeline.getSchemaRegistry().getSchemaCoder(getTypeDescriptor());
            } catch (NoSuchSchemaException e) {
                return null;
            }
        }

        private Coder<T> getFromCoderRegistry(Pipeline pipeline) {
            try {
                return pipeline.getCoderRegistry().getCoder(getTypeDescriptor());
            } catch (CannotProvideCoderException e) {
                return null;
            }
        }

        // FIXME: this is public only for the sake of testing, TODO: redesign test so this is
        // private
        public Queue initializeQueueForTopic(String jobName, SempClientFactory sempClientFactory) {
            Queue q;
            if (getQueue() != null) {
                q = getQueue();
            } else {
                String queueName = String.format("queue-%s-%s", getTopic(), jobName);
                try {
                    String topicName = Objects.requireNonNull(getTopic()).getName();
                    q = sempClientFactory.create().createQueueForTopic(queueName, topicName);
                    LOG.info(
                            "SolaceIO.Read: A new queue {} was created. The Queue will not be"
                                + " deleted when this job finishes. Make sure to remove it yourself"
                                + " when not needed.",
                            q.getName());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return q;
        }

        private void validate() {
            Preconditions.checkState(
                    getSempClientFactory() != null,
                    "SolaceIO.Read: brokerServiceFactory must not be null.");
            Preconditions.checkState(
                    getSessionServiceFactory() != null,
                    "SolaceIO.Read: SessionServiceFactory must not be null.");
            Preconditions.checkState(
                    getParseFn() != null,
                    "SolaceIO.Read: parseFn must be set or use the `Read.readSolaceRecords()`"
                            + " method");
            Preconditions.checkState(
                    getTimestampFn() != null,
                    "SolaceIO.Read: timestamp function must be set or use the"
                            + " `Read.readSolaceRecords()` method");
            Preconditions.checkState(
                    (getQueue() == null ^ getTopic() == null),
                    "SolaceIO.Read: One of the Solace {Queue, Topic} must be set.");
        }
    }
}
