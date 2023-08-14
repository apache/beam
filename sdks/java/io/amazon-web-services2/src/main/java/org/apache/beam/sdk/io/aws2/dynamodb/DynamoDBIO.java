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
package org.apache.beam.sdk.io.aws2.dynamodb;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.aws2.common.ClientBuilderFactory;
import org.apache.beam.sdk.io.aws2.common.ClientConfiguration;
import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

/**
 * IO to read from and write to <a href="https://aws.amazon.com/dynamodb/">DynamoDB</a> tables.
 *
 * <h3>Reading from DynamoDB</h3>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * PCollection<List<Map<String, AttributeValue>>> output =
 *   pipeline.apply(
 *     DynamoDBIO.<List<Map<String, AttributeValue>>>read()
 *       .withScanRequestFn(in -> ScanRequest.builder().tableName(tableName).totalSegments(1).build())
 *       .items()); // ScanResponse items mapper
 * }</pre>
 *
 * <p>At a minimum you have to provide:
 *
 * <ul>
 *   <li>a {@code scanRequestFn} providing the {@link ScanRequest} instance; {@code table name} and
 *       {@code total segments} are required. Note: Choose {@code total segments} according to the
 *       number of workers used.
 *   <li>a {@code scanResponseMapperFn} to map the {@link ScanResponse} to the expected output type,
 *       such as {@link Read#items()}.
 * </ul>
 *
 * <h3>Writing to DynamoDB</h3>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * PCollection<T> data = ...;
 * SerializableFunction<T, WriteRequest> requestBuilder = ...;
 * data.apply(
 *   DynamoDBIO.<WriteRequest>write()
 *     .withWriteRequestMapperFn(t -> KV.of(tableName, requestBuilder.apply(t))));
 * }</pre>
 *
 * <p>At a minimum you have to provide a {@code writeRequestMapperFn} to map each element into a
 * {@link KV} of {@code table name} and {@link WriteRequest}.
 *
 * <p><b>Note:</b> AWS does not allow writing duplicate keys within a single batch operation. If
 * primary keys possibly repeat in your stream (i.e. an upsert stream), you may encounter a
 * `ValidationError`. To address this you have to provide the key names corresponding to your
 * primary key using {@link Write#withDeduplicateKeys(List)}. Based on these keys only the last
 * observed element is kept. Nevertheless, if no deduplication keys are provided, identical elements
 * are still deduplicated.
 *
 * <h3>Configuration of AWS clients</h3>
 *
 * <p>AWS clients for all AWS IOs can be configured using {@link AwsOptions}, e.g. {@code
 * --awsRegion=us-west-1}. {@link AwsOptions} contain reasonable defaults based on default providers
 * for {@link Region} and {@link AwsCredentialsProvider}.
 *
 * <p>If you require more advanced configuration, you may change the {@link ClientBuilderFactory}
 * using {@link AwsOptions#setClientBuilderFactory(Class)}.
 *
 * <p>Configuration for a specific IO can be overwritten using {@code withClientConfiguration()},
 * which also allows to configure the retry behavior for the respective IO.
 *
 * <h4>Retries</h4>
 *
 * <p>Retries for failed requests can be configured using {@link
 * ClientConfiguration.Builder#retry(Consumer)} and are handled by the AWS SDK unless there's a
 * partial success (batch requests). The SDK uses a backoff strategy with equal jitter for computing
 * the delay before the next retry.
 *
 * <p><b>Note:</b> Once retries are exhausted the error is surfaced to the runner which <em>may</em>
 * then opt to retry the current partition in entirety or abort if the max number of retries of the
 * runner is reached.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public final class DynamoDBIO {
  public static <T> Read<T> read() {
    return new AutoValue_DynamoDBIO_Read.Builder<T>()
        .setClientConfiguration(ClientConfiguration.builder().build())
        .build();
  }

  public static <T> Write<T> write() {
    return new AutoValue_DynamoDBIO_Write.Builder<T>()
        .setClientConfiguration(ClientConfiguration.builder().build())
        .setDeduplicateKeys(new ArrayList<>())
        .build();
  }

  /**
   * Read data from DynamoDB using {@link #getScanRequestFn()} and emit an element of type {@link T}
   * for each {@link ScanResponse} using the mapping function {@link #getScanResponseMapperFn()}.
   */
  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {

    abstract ClientConfiguration getClientConfiguration();

    abstract @Nullable SerializableFunction<Void, ScanRequest> getScanRequestFn();

    abstract @Nullable Integer getSegmentId();

    abstract @Nullable SerializableFunction<ScanResponse, T> getScanResponseMapperFn();

    abstract @Nullable Coder<T> getCoder();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {

      abstract Builder<T> setClientConfiguration(ClientConfiguration config);

      abstract Builder<T> setScanRequestFn(SerializableFunction<Void, ScanRequest> fn);

      abstract Builder<T> setSegmentId(Integer segmentId);

      abstract Builder<T> setScanResponseMapperFn(SerializableFunction<ScanResponse, T> fn);

      abstract Builder<T> setCoder(Coder<T> coder);

      abstract Read<T> build();
    }

    /** Configuration of DynamoDB client. */
    public Read<T> withClientConfiguration(ClientConfiguration config) {
      checkArgument(config != null, "ClientConfiguration cannot be null");
      return toBuilder().setClientConfiguration(config).build();
    }

    /**
     * Can't pass ScanRequest object directly from client since this object is not full
     * serializable.
     */
    public Read<T> withScanRequestFn(SerializableFunction<Void, ScanRequest> fn) {
      return toBuilder().setScanRequestFn(fn).build();
    }

    private Read<T> withSegmentId(Integer segmentId) {
      checkArgument(segmentId != null, "segmentId can not be null");
      return toBuilder().setSegmentId(segmentId).build();
    }

    public Read<T> withScanResponseMapperFn(
        SerializableFunction<ScanResponse, T> scanResultMapperFn) {
      checkArgument(scanResultMapperFn != null, "scanResultMapper can not be null");
      return toBuilder().setScanResponseMapperFn(scanResultMapperFn).build();
    }

    public Read<List<Map<String, AttributeValue>>> items() {
      // safe cast as both mapper and coder are updated accordingly
      Read<List<Map<String, AttributeValue>>> self = (Read<List<Map<String, AttributeValue>>>) this;
      return self.withScanResponseMapperFn(new ItemsMapper())
          .withCoder(ListCoder.of(MapCoder.of(StringUtf8Coder.of(), AttributeValueCoder.of())));
    }

    public Read<T> withCoder(Coder<T> coder) {
      checkArgument(coder != null, "coder can not be null");
      return toBuilder().setCoder(coder).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      checkArgument(getScanResponseMapperFn() != null, "withScanResponseMapperFn() is required");
      checkArgument(getScanRequestFn() != null, "withScanRequestFn() is required");
      ScanRequest scanRequest = getScanRequestFn().apply(null);
      checkArgument(
          (scanRequest.totalSegments() != null && scanRequest.totalSegments() > 0),
          "TotalSegments is required with withScanRequestFn() and greater zero");

      AwsOptions awsOptions = input.getPipeline().getOptions().as(AwsOptions.class);
      ClientBuilderFactory.validate(awsOptions, getClientConfiguration());

      PCollection<Read<T>> splits =
          input.apply("Create", Create.of(this)).apply("Split", ParDo.of(new SplitFn<>()));
      splits.setCoder(SerializableCoder.of(new TypeDescriptor<Read<T>>() {}));

      PCollection<T> output =
          splits
              .apply("Reshuffle", Reshuffle.viaRandomKey())
              .apply("Read", ParDo.of(new ReadFn<>()));
      output.setCoder(getCoder());
      return output;
    }

    /** A {@link DoFn} to split {@link Read} elements by segment id. */
    private static class SplitFn<T> extends DoFn<Read<T>, Read<T>> {
      @ProcessElement
      public void processElement(@Element Read<T> spec, OutputReceiver<Read<T>> out) {
        ScanRequest scanRequest = spec.getScanRequestFn().apply(null);
        for (int i = 0; i < scanRequest.totalSegments(); i++) {
          out.output(spec.withSegmentId(i));
        }
      }
    }

    /** A {@link DoFn} executing the ScanRequest to read from DynamoDb. */
    private static class ReadFn<T> extends DoFn<Read<T>, T> {
      private DynamoDbClient buildClient(Read<T> spec, AwsOptions opts) {
        return ClientBuilderFactory.buildClient(
            opts, DynamoDbClient.builder(), spec.getClientConfiguration());
      }

      @ProcessElement
      public void processElement(
          @Element Read<T> spec, OutputReceiver<T> out, PipelineOptions opts) {
        try (DynamoDbClient client = buildClient(spec, opts.as(AwsOptions.class))) {
          Map<String, AttributeValue> lastEvaluatedKey = null;

          do {
            ScanRequest scanRequest = spec.getScanRequestFn().apply(null);
            ScanRequest scanRequestWithSegment =
                scanRequest
                    .toBuilder()
                    .segment(spec.getSegmentId())
                    .exclusiveStartKey(lastEvaluatedKey)
                    .build();

            ScanResponse scanResponse = client.scan(scanRequestWithSegment);
            out.output(spec.getScanResponseMapperFn().apply(scanResponse));
            lastEvaluatedKey = scanResponse.lastEvaluatedKey();
          } while (lastEvaluatedKey != null
              && !lastEvaluatedKey.isEmpty()); // iterate until all records are fetched
        }
      }
    }

    static final class ItemsMapper
        implements SerializableFunction<ScanResponse, List<Map<String, AttributeValue>>> {
      @Override
      public List<Map<String, AttributeValue>> apply(@Nullable ScanResponse scanResponse) {
        if (scanResponse == null) {
          return Collections.emptyList();
        }
        return scanResponse.items();
      }
    }
  }

  /** Write a PCollection<T> data into DynamoDB. */
  @AutoValue
  public abstract static class Write<T> extends PTransform<PCollection<T>, PCollection<Void>> {

    abstract ClientConfiguration getClientConfiguration();

    abstract @Nullable SerializableFunction<T, KV<String, WriteRequest>> getWriteItemMapperFn();

    abstract List<String> getDeduplicateKeys();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setClientConfiguration(ClientConfiguration config);

      abstract Builder<T> setWriteItemMapperFn(
          SerializableFunction<T, KV<String, WriteRequest>> writeItemMapperFn);

      abstract Builder<T> setDeduplicateKeys(List<String> deduplicateKeys);

      abstract Write<T> build();
    }

    /** Configuration of DynamoDB client. */
    public Write<T> withClientConfiguration(ClientConfiguration config) {
      checkArgument(config != null, "ClientConfiguration cannot be null");
      return toBuilder().setClientConfiguration(config).build();
    }

    public Write<T> withWriteRequestMapperFn(
        SerializableFunction<T, KV<String, WriteRequest>> writeItemMapperFn) {
      return toBuilder().setWriteItemMapperFn(writeItemMapperFn).build();
    }

    public Write<T> withDeduplicateKeys(List<String> deduplicateKeys) {
      return toBuilder().setDeduplicateKeys(deduplicateKeys).build();
    }

    @Override
    public PCollection<Void> expand(PCollection<T> input) {
      checkNotNull(getClientConfiguration(), "clientConfiguration cannot be null");
      AwsOptions awsOptions = input.getPipeline().getOptions().as(AwsOptions.class);
      ClientBuilderFactory.validate(awsOptions, getClientConfiguration());

      return input.apply(ParDo.of(new WriteFn<>(this)));
    }

    static class WriteFn<T> extends DoFn<T, Void> {
      private static final String RESUME_ERROR_LOG =
          "Error writing remaining unprocessed items to DynamoDB: {}";

      private static final String ERROR_UNPROCESSED_ITEMS =
          "Error writing to DynamoDB. Unprocessed items remaining";

      private transient FluentBackoff resumeBackoff; // resume from partial failures

      private static final Logger LOG = LoggerFactory.getLogger(WriteFn.class);
      private static final Counter DYNAMO_DB_WRITE_FAILURES =
          Metrics.counter(WriteFn.class, "DynamoDB_Write_Failures");

      private static final int BATCH_SIZE = 25;
      private transient DynamoDbClient client;
      private final Write<T> spec;
      private Map<KV<String, Map<String, AttributeValue>>, KV<String, WriteRequest>> batch;

      WriteFn(Write<T> spec) {
        this.spec = spec;
      }

      @Setup
      public void setup(PipelineOptions options) {
        ClientConfiguration clientConfig = spec.getClientConfiguration();
        AwsOptions awsOpts = options.as(AwsOptions.class);
        client = ClientBuilderFactory.buildClient(awsOpts, DynamoDbClient.builder(), clientConfig);

        // resume from partial failures
        resumeBackoff = FluentBackoff.DEFAULT.withMaxRetries(BATCH_SIZE);
        if (clientConfig != null && clientConfig.retry() != null) {
          Duration baseBackoff = clientConfig.retry().throttledBaseBackoff();
          Duration maxBackoff = clientConfig.retry().maxBackoff();
          if (baseBackoff != null) {
            resumeBackoff = resumeBackoff.withInitialBackoff(baseBackoff);
          }
          if (maxBackoff != null) {
            resumeBackoff = resumeBackoff.withMaxBackoff(maxBackoff);
          }
        }
      }

      @StartBundle
      public void startBundle(StartBundleContext context) {
        batch = new HashMap<>();
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        final KV<String, WriteRequest> writeRequest =
            spec.getWriteItemMapperFn().apply(context.element());
        batch.put(
            KV.of(writeRequest.getKey(), extractDeduplicateKeyValues(writeRequest.getValue())),
            writeRequest);
        if (batch.size() >= BATCH_SIZE) {
          flushBatch();
        }
      }

      private Map<String, AttributeValue> extractDeduplicateKeyValues(WriteRequest request) {
        List<String> deduplicationKeys = spec.getDeduplicateKeys();
        Map<String, AttributeValue> attributes = Collections.emptyMap();

        if (request.putRequest() != null) {
          attributes = request.putRequest().item();
        } else if (request.deleteRequest() != null) {
          attributes = request.deleteRequest().key();
        }

        if (attributes.isEmpty() || deduplicationKeys.isEmpty()) {
          return attributes;
        }

        return attributes.entrySet().stream()
            .filter(entry -> deduplicationKeys.contains(entry.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      }

      @FinishBundle
      public void finishBundle() throws Exception {
        flushBatch();
      }

      private void flushBatch() throws IOException, InterruptedException {
        if (batch.isEmpty()) {
          return;
        }
        try {
          // Group values KV<tableName, writeRequest> by tableName
          // Note: The original order of arrival is lost reading the map entries.
          Map<String, List<WriteRequest>> writesPerTable =
              batch.values().stream()
                  .collect(groupingBy(KV::getKey, mapping(KV::getValue, toList())));

          // Backoff used to resume from partial failures
          BackOff resume = resumeBackoff.backoff();
          do {
            BatchWriteItemRequest batchRequest =
                BatchWriteItemRequest.builder().requestItems(writesPerTable).build();
            // If unprocessed items remain, we have to resume the operation (with backoff)
            writesPerTable = client.batchWriteItem(batchRequest).unprocessedItems();
          } while (!writesPerTable.isEmpty() && BackOffUtils.next(Sleeper.DEFAULT, resume));

          if (!writesPerTable.isEmpty()) {
            DYNAMO_DB_WRITE_FAILURES.inc();
            LOG.error(RESUME_ERROR_LOG, writesPerTable);
            throw new IOException(ERROR_UNPROCESSED_ITEMS);
          }
        } finally {
          batch.clear();
        }
      }

      @Teardown
      public void tearDown() {
        if (client != null) {
          client.close();
        }
      }
    }
  }
}
