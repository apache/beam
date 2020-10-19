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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
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
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.http.HttpStatus;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

/**
 * {@link PTransform}s to read/write from/to <a href="https://aws.amazon.com/dynamodb/">Amazon
 * DynamoDB</a>.
 *
 * <h3>Reading from DynamoDB</h3>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * PCollection<List<Map<String, AttributeValue>>> output =
 *     pipeline.apply(
 *             DynamoDBIO.<List<Map<String, AttributeValue>>>read()
 *                 .withDynamoDbClientProvider(new BasicDynamoDbClientProvider(dynamoDbClientProvider, region))
 *                 .withScanRequestFn(
 *                     (SerializableFunction<Void, ScanRequest>)
 *                         input -> new ScanRequest(tableName).withTotalSegments(1))
 *                 .items());
 * }</pre>
 *
 * <p>As a client, you need to provide at least the following things:
 *
 * <ul>
 *   <li>Specify DynamoDbClientProvider. You can pass on the default one BasicDynamoDbClientProvider
 *   <li>ScanRequestFn, which you build a ScanRequest object with at least table name and total
 *       number of segment. Note This number should base on the number of your workers
 * </ul>
 *
 * {@link PTransform}s to read/write from/to <a
 * href="https://aws.amazon.com/dynamodb/">DynamoDB</a>.
 *
 * <h3>Writing to DynamoDB</h3>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * PCollection<T> data = ...;
 * data.apply(
 *           DynamoDBIO.<WriteRequest>write()
 *               .withWriteRequestMapperFn(
 *                   (SerializableFunction<T, KV<String, WriteRequest>>)
 *                       //Transforming your T data into KV<String, WriteRequest>
 *                       t -> KV.of(tableName, writeRequest))
 *               .withRetryConfiguration(
 *                     DynamoDBIO.RetryConfiguration.builder()
 *                         .setMaxAttempts(5)
 *                         .setMaxDuration(Duration.standardMinutes(1))
 *                         .build())
 *               .withDynamoDbClientProvider(new BasicDynamoDbClientProvider(dynamoDbClientProvider, region));
 * }</pre>
 *
 * <p>As a client, you need to provide at least the following things:
 *
 * <ul>
 *   <li>Retry configuration
 *   <li>Specify DynamoDbClientProvider. You can pass on the default one BasicDynamoDbClientProvider
 *   <li>Mapper function with a table name to map or transform your object into KV<tableName,
 *       writeRequest>
 * </ul>
 */
@Experimental(Kind.SOURCE_SINK)
public final class DynamoDBIO {
  public static <T> Read<T> read() {
    return new AutoValue_DynamoDBIO_Read.Builder().build();
  }

  public static <T> Write<T> write() {
    return new AutoValue_DynamoDBIO_Write.Builder().build();
  }

  /** Read data from DynamoDB and return ScanResult. */
  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {

    abstract @Nullable DynamoDbClientProvider getDynamoDbClientProvider();

    abstract @Nullable SerializableFunction<Void, ScanRequest> getScanRequestFn();

    abstract @Nullable Integer getSegmentId();

    abstract @Nullable SerializableFunction<ScanResponse, T> getScanResponseMapperFn();

    abstract @Nullable Coder<T> getCoder();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {

      abstract Builder<T> setDynamoDbClientProvider(DynamoDbClientProvider dynamoDbClientProvider);

      abstract Builder<T> setScanRequestFn(SerializableFunction<Void, ScanRequest> fn);

      abstract Builder<T> setSegmentId(Integer segmentId);

      abstract Builder<T> setScanResponseMapperFn(
          SerializableFunction<ScanResponse, T> scanResponseMapperFn);

      abstract Builder<T> setCoder(Coder<T> coder);

      abstract Read<T> build();
    }

    public Read<T> withDynamoDbClientProvider(DynamoDbClientProvider dynamoDbClientProvider) {
      return toBuilder().setDynamoDbClientProvider(dynamoDbClientProvider).build();
    }

    public Read<T> withDynamoDbClientProvider(
        AwsCredentialsProvider credentialsProvider, String region, URI serviceEndpoint) {
      return withDynamoDbClientProvider(
          new BasicDynamoDbClientProvider(credentialsProvider, region, serviceEndpoint));
    }

    public Read<T> withDynamoDbClientProvider(
        AwsCredentialsProvider credentialsProvider, String region) {
      return withDynamoDbClientProvider(credentialsProvider, region, null);
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
      return withScanResponseMapperFn(new ItemsMapper())
          .withCoder(ListCoder.of(MapCoder.of(StringUtf8Coder.of(), AttributeValueCoder.of())));
    }

    public Read<T> withCoder(Coder<T> coder) {
      checkArgument(coder != null, "coder can not be null");
      return toBuilder().setCoder(coder).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      checkArgument((getScanRequestFn() != null), "withScanRequestFn() is required");
      checkArgument(
          (getDynamoDbClientProvider() != null), "withDynamoDbClientProvider() is required");
      ScanRequest scanRequest = getScanRequestFn().apply(null);
      checkArgument(
          (scanRequest.totalSegments() != null && scanRequest.totalSegments() > 0),
          "TotalSegments is required with withScanRequestFn() and greater zero");

      PCollection<Read<T>> splits =
          (PCollection<Read<T>>)
              input.apply("Create", Create.of(this)).apply("Split", ParDo.of(new SplitFn()));
      splits.setCoder(SerializableCoder.of(new TypeDescriptor<Read<T>>() {}));

      PCollection<T> output =
          (PCollection<T>)
              splits
                  .apply("Reshuffle", Reshuffle.viaRandomKey())
                  .apply("Read", ParDo.of(new ReadFn()));
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
      @ProcessElement
      public void processElement(@Element Read<T> spec, OutputReceiver<T> out) {
        DynamoDbClient client = spec.getDynamoDbClientProvider().getDynamoDbClient();

        ScanRequest scanRequest = spec.getScanRequestFn().apply(null);
        ScanRequest scanRequestWithSegment =
            scanRequest.toBuilder().segment(spec.getSegmentId()).build();

        ScanResponse scanResponse = client.scan(scanRequestWithSegment);
        out.output(spec.getScanResponseMapperFn().apply(scanResponse));
      }
    }

    static final class ItemsMapper<T>
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

  /**
   * A POJO encapsulating a configuration for retry behavior when issuing requests to DynamoDB. A
   * retry will be attempted until the maxAttempts or maxDuration is exceeded, whichever comes
   * first, for any of the following exceptions:
   *
   * <ul>
   *   <li>{@link IOException}
   * </ul>
   */
  @AutoValue
  public abstract static class RetryConfiguration implements Serializable {
    @VisibleForTesting
    static final RetryPredicate DEFAULT_RETRY_PREDICATE = new DefaultRetryPredicate();

    abstract int getMaxAttempts();

    abstract Duration getMaxDuration();

    abstract RetryPredicate getRetryPredicate();

    abstract Builder toBuilder();

    public static Builder builder() {
      return new AutoValue_DynamoDBIO_RetryConfiguration.Builder()
          .setRetryPredicate(DEFAULT_RETRY_PREDICATE);
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setMaxAttempts(int maxAttempts);

      public abstract Builder setMaxDuration(Duration maxDuration);

      abstract Builder setRetryPredicate(RetryPredicate retryPredicate);

      abstract RetryConfiguration autoBuild();

      public RetryConfiguration build() {
        RetryConfiguration configuration = autoBuild();
        checkArgument(configuration.getMaxAttempts() > 0, "maxAttempts should be greater than 0");
        checkArgument(
            configuration.getMaxDuration() != null
                && configuration.getMaxDuration().isLongerThan(Duration.ZERO),
            "maxDuration should be greater than 0");
        return configuration;
      }
    }

    /**
     * An interface used to control if we retry the BatchWriteItemRequest call when a {@link
     * Throwable} occurs. If {@link RetryPredicate#test(Object)} returns true, {@link Write} tries
     * to resend the requests to the DynamoDB server if the {@link RetryConfiguration} permits it.
     */
    @FunctionalInterface
    interface RetryPredicate extends Predicate<Throwable>, Serializable {}

    private static class DefaultRetryPredicate implements RetryPredicate {
      private static final ImmutableSet<Integer> ELIGIBLE_CODES =
          ImmutableSet.of(HttpStatus.SC_SERVICE_UNAVAILABLE);

      @Override
      public boolean test(Throwable throwable) {
        return (throwable instanceof IOException
            || (throwable instanceof DynamoDbException)
            || (throwable instanceof DynamoDbException
                && ELIGIBLE_CODES.contains(((DynamoDbException) throwable).statusCode())));
      }
    }
  }

  /** Write a PCollection<T> data into DynamoDB. */
  @AutoValue
  public abstract static class Write<T> extends PTransform<PCollection<T>, PCollection<Void>> {

    abstract @Nullable DynamoDbClientProvider getDynamoDbClientProvider();

    abstract @Nullable RetryConfiguration getRetryConfiguration();

    abstract @Nullable SerializableFunction<T, KV<String, WriteRequest>> getWriteItemMapperFn();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {

      abstract Builder<T> setDynamoDbClientProvider(DynamoDbClientProvider dynamoDbClientProvider);

      abstract Builder<T> setRetryConfiguration(RetryConfiguration retryConfiguration);

      abstract Builder<T> setWriteItemMapperFn(
          SerializableFunction<T, KV<String, WriteRequest>> writeItemMapperFn);

      abstract Write<T> build();
    }

    public Write<T> withDynamoDbClientProvider(DynamoDbClientProvider dynamoDbClientProvider) {
      return toBuilder().setDynamoDbClientProvider(dynamoDbClientProvider).build();
    }

    public Write<T> withDynamoDbClientProvider(
        AwsCredentialsProvider credentialsProvider, String region, URI serviceEndpoint) {
      return withDynamoDbClientProvider(
          new BasicDynamoDbClientProvider(credentialsProvider, region, serviceEndpoint));
    }

    public Write<T> withDynamoDbClientProvider(
        AwsCredentialsProvider credentialsProvider, String region) {
      return withDynamoDbClientProvider(credentialsProvider, region, null);
    }

    /**
     * Provides configuration to retry a failed request to publish a set of records to DynamoDb.
     * Users should consider that retrying might compound the underlying problem which caused the
     * initial failure. Users should also be aware that once retrying is exhausted the error is
     * surfaced to the runner which <em>may</em> then opt to retry the current partition in entirety
     * or abort if the max number of retries of the runner is completed. Retrying uses an
     * exponential backoff algorithm, with minimum backoff of 5 seconds and then surfacing the error
     * once the maximum number of retries or maximum configuration duration is exceeded.
     *
     * <p>Example use:
     *
     * <pre>{@code
     * DynamoDBIO.write()
     *  .withRetryConfiguration(
     *      DynamoDBIO.RetryConfiguration.builder()
     *          .setMaxAttempts(4)
     *          .setMaxDuration(Duration.standardMinutes(1))
     *          .build())
     *   ...
     * }</pre>
     *
     * @param retryConfiguration the rules which govern the retry behavior
     * @return the {@link Write} with retrying configured
     */
    public Write<T> withRetryConfiguration(RetryConfiguration retryConfiguration) {
      checkArgument(retryConfiguration != null, "retryConfiguration is required");
      return toBuilder().setRetryConfiguration(retryConfiguration).build();
    }

    public Write<T> withWriteRequestMapperFn(
        SerializableFunction<T, KV<String, WriteRequest>> writeItemMapperFn) {
      return toBuilder().setWriteItemMapperFn(writeItemMapperFn).build();
    }

    @Override
    public PCollection<Void> expand(PCollection<T> input) {
      return input.apply(ParDo.of(new WriteFn<>(this)));
    }

    static class WriteFn<T> extends DoFn<T, Void> {
      @VisibleForTesting
      static final String RETRY_ATTEMPT_LOG = "Error writing to DynamoDB. Retry attempt[%d]";

      private static final Duration RETRY_INITIAL_BACKOFF = Duration.standardSeconds(5);
      private transient FluentBackoff retryBackoff; // defaults to no retries
      private static final Logger LOG = LoggerFactory.getLogger(WriteFn.class);
      private static final Counter DYNAMO_DB_WRITE_FAILURES =
          Metrics.counter(WriteFn.class, "DynamoDB_Write_Failures");

      private static final int BATCH_SIZE = 25;
      private transient DynamoDbClient client;
      private final Write spec;
      private List<KV<String, WriteRequest>> batch;

      WriteFn(Write spec) {
        this.spec = spec;
      }

      @Setup
      public void setup() {
        client = spec.getDynamoDbClientProvider().getDynamoDbClient();
        retryBackoff =
            FluentBackoff.DEFAULT
                .withMaxRetries(0) // default to no retrying
                .withInitialBackoff(RETRY_INITIAL_BACKOFF);
        if (spec.getRetryConfiguration() != null) {
          retryBackoff =
              retryBackoff
                  .withMaxRetries(spec.getRetryConfiguration().getMaxAttempts() - 1)
                  .withMaxCumulativeBackoff(spec.getRetryConfiguration().getMaxDuration());
        }
      }

      @StartBundle
      public void startBundle(StartBundleContext context) {
        batch = new ArrayList<>();
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        final KV<String, WriteRequest> writeRequest =
            (KV<String, WriteRequest>) spec.getWriteItemMapperFn().apply(context.element());
        batch.add(writeRequest);
        if (batch.size() >= BATCH_SIZE) {
          flushBatch();
        }
      }

      @FinishBundle
      public void finishBundle(FinishBundleContext context) throws Exception {
        flushBatch();
      }

      private void flushBatch() throws IOException, InterruptedException {
        if (batch.isEmpty()) {
          return;
        }

        try {
          // Since each element is a KV<tableName, writeRequest> in the batch, we need to group them
          // by tableName
          Map<String, List<WriteRequest>> mapTableRequest =
              batch.stream()
                  .collect(
                      Collectors.groupingBy(
                          KV::getKey, Collectors.mapping(KV::getValue, Collectors.toList())));

          BatchWriteItemRequest batchRequest =
              BatchWriteItemRequest.builder().requestItems(mapTableRequest).build();

          Sleeper sleeper = Sleeper.DEFAULT;
          BackOff backoff = retryBackoff.backoff();
          int attempt = 0;
          while (true) {
            attempt++;
            try {
              client.batchWriteItem(batchRequest);
              break;
            } catch (Exception ex) {
              // Fail right away if there is no retry configuration
              if (spec.getRetryConfiguration() == null
                  || !spec.getRetryConfiguration().getRetryPredicate().test(ex)) {
                DYNAMO_DB_WRITE_FAILURES.inc();
                LOG.info(
                    "Unable to write batch items {}.", batchRequest.requestItems().entrySet(), ex);
                throw new IOException("Error writing to DynamoDB (no attempt made to retry)", ex);
              }

              if (!BackOffUtils.next(sleeper, backoff)) {
                throw new IOException(
                    String.format(
                        "Error writing to DynamoDB after %d attempt(s). No more attempts allowed",
                        attempt),
                    ex);
              } else {
                // Note: this used in test cases to verify behavior
                LOG.warn(String.format(RETRY_ATTEMPT_LOG, attempt), ex);
              }
            }
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
