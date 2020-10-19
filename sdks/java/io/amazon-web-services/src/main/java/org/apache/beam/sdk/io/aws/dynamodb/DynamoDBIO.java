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
package org.apache.beam.sdk.io.aws.dynamodb;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.Serializable;
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

/**
 * {@link PTransform}s to read/write from/to <a href="https://aws.amazon.com/dynamodb/">Amazon
 * DynamoDB</a>.
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
 *                    DynamoDBIO.RetryConfiguration.create(5, Duration.standardMinutes(1)))
 *               .withAwsClientsProvider(new BasicDynamoDbProvider(accessKey, secretKey, region));
 * }</pre>
 *
 * <p>As a client, you need to provide at least the following things:
 *
 * <ul>
 *   <li>Retry configuration
 *   <li>Specify AwsClientsProvider. You can pass on the default one BasicDynamoDbProvider
 *   <li>Mapper function with a table name to map or transform your object into KV<tableName,
 *       writeRequest>
 * </ul>
 *
 * <h3>Reading from DynamoDB</h3>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * PCollection<List<Map<String, AttributeValue>>> output =
 *     pipeline.apply(
 *             DynamoDBIO.<List<Map<String, AttributeValue>>>read()
 *                 .withAwsClientsProvider(new BasicDynamoDBProvider(accessKey, secretKey, region))
 *                 .withScanRequestFn(
 *                     (SerializableFunction<Void, ScanRequest>)
 *                         input -> new ScanRequest(tableName).withTotalSegments(1))
 *                 .items());
 * }</pre>
 *
 * <p>As a client, you need to provide at least the following things:
 *
 * <ul>
 *   <li>Specify AwsClientsProvider. You can pass on the default one BasicDynamoDBProvider
 *   <li>ScanRequestFn, which you build a ScanRequest object with at least table name and total
 *       number of segment. Note This number should base on the number of your workers
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

    abstract @Nullable AwsClientsProvider getAwsClientsProvider();

    abstract @Nullable SerializableFunction<Void, ScanRequest> getScanRequestFn();

    abstract @Nullable Integer getSegmentId();

    abstract @Nullable SerializableFunction<ScanResult, T> getScanResultMapperFn();

    abstract @Nullable Coder<T> getCoder();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {

      abstract Builder<T> setAwsClientsProvider(AwsClientsProvider awsClientsProvider);

      abstract Builder<T> setScanRequestFn(SerializableFunction<Void, ScanRequest> fn);

      abstract Builder<T> setSegmentId(Integer segmentId);

      abstract Builder<T> setScanResultMapperFn(
          SerializableFunction<ScanResult, T> scanResultMapperFn);

      abstract Builder<T> setCoder(Coder<T> coder);

      abstract Read<T> build();
    }

    public Read<T> withAwsClientsProvider(AwsClientsProvider awsClientsProvider) {
      return toBuilder().setAwsClientsProvider(awsClientsProvider).build();
    }

    public Read<T> withAwsClientsProvider(
        String awsAccessKey, String awsSecretKey, Regions region, String serviceEndpoint) {
      return withAwsClientsProvider(
          new BasicDynamoDBProvider(awsAccessKey, awsSecretKey, region, serviceEndpoint));
    }

    public Read<T> withAwsClientsProvider(
        String awsAccessKey, String awsSecretKey, Regions region) {
      return withAwsClientsProvider(awsAccessKey, awsSecretKey, region, null);
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

    public Read<T> withScanResultMapperFn(SerializableFunction<ScanResult, T> scanResultMapperFn) {
      checkArgument(scanResultMapperFn != null, "scanResultMapper can not be null");
      return toBuilder().setScanResultMapperFn(scanResultMapperFn).build();
    }

    public Read<List<Map<String, AttributeValue>>> items() {
      return withScanResultMapperFn(new DynamoDBIO.Read.ItemsMapper())
          .withCoder(ListCoder.of(MapCoder.of(StringUtf8Coder.of(), AttributeValueCoder.of())));
    }

    public Read<T> withCoder(Coder<T> coder) {
      checkArgument(coder != null, "coder can not be null");
      return toBuilder().setCoder(coder).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      checkArgument((getScanRequestFn() != null), "withScanRequestFn() is required");
      checkArgument((getAwsClientsProvider() != null), "withAwsClientsProvider() is required");
      ScanRequest scanRequest = getScanRequestFn().apply(null);
      checkArgument(
          (scanRequest.getTotalSegments() != null && scanRequest.getTotalSegments() > 0),
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
        for (int i = 0; i < scanRequest.getTotalSegments(); i++) {
          out.output(spec.withSegmentId(i));
        }
      }
    }

    /** A {@link DoFn} executing the ScanRequest to read from DynamoDB. */
    private static class ReadFn<T> extends DoFn<Read<T>, T> {
      @ProcessElement
      public void processElement(@Element Read<T> spec, OutputReceiver<T> out) {
        AmazonDynamoDB client = spec.getAwsClientsProvider().createDynamoDB();
        ScanRequest scanRequest = spec.getScanRequestFn().apply(null);
        scanRequest.setSegment(spec.getSegmentId());
        ScanResult scanResult = client.scan(scanRequest);
        out.output(spec.getScanResultMapperFn().apply(scanResult));
      }
    }

    static final class ItemsMapper<T>
        implements SerializableFunction<ScanResult, List<Map<String, AttributeValue>>> {
      @Override
      public List<Map<String, AttributeValue>> apply(@Nullable ScanResult scanResult) {
        if (scanResult == null) {
          return Collections.emptyList();
        }
        return scanResult.getItems();
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

    abstract DynamoDBIO.RetryConfiguration.RetryPredicate getRetryPredicate();

    abstract DynamoDBIO.RetryConfiguration.Builder builder();

    public static DynamoDBIO.RetryConfiguration create(int maxAttempts, Duration maxDuration) {
      checkArgument(maxAttempts > 0, "maxAttempts should be greater than 0");
      checkArgument(
          maxDuration != null && maxDuration.isLongerThan(Duration.ZERO),
          "maxDuration should be greater than 0");
      return new AutoValue_DynamoDBIO_RetryConfiguration.Builder()
          .setMaxAttempts(maxAttempts)
          .setMaxDuration(maxDuration)
          .setRetryPredicate(DEFAULT_RETRY_PREDICATE)
          .build();
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract DynamoDBIO.RetryConfiguration.Builder setMaxAttempts(int maxAttempts);

      abstract DynamoDBIO.RetryConfiguration.Builder setMaxDuration(Duration maxDuration);

      abstract DynamoDBIO.RetryConfiguration.Builder setRetryPredicate(
          RetryPredicate retryPredicate);

      abstract DynamoDBIO.RetryConfiguration build();
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
            || (throwable instanceof AmazonDynamoDBException)
            || (throwable instanceof AmazonDynamoDBException
                && ELIGIBLE_CODES.contains(((AmazonDynamoDBException) throwable).getStatusCode())));
      }
    }
  }

  /** Write a PCollection<T> data into DynamoDB. */
  @AutoValue
  public abstract static class Write<T> extends PTransform<PCollection<T>, PCollection<Void>> {

    abstract @Nullable AwsClientsProvider getAwsClientsProvider();

    abstract @Nullable RetryConfiguration getRetryConfiguration();

    abstract @Nullable SerializableFunction<T, KV<String, WriteRequest>> getWriteItemMapperFn();

    abstract Builder<T> builder();

    @AutoValue.Builder
    abstract static class Builder<T> {

      abstract Builder<T> setAwsClientsProvider(AwsClientsProvider awsClientsProvider);

      abstract Builder<T> setRetryConfiguration(RetryConfiguration retryConfiguration);

      abstract Builder<T> setWriteItemMapperFn(
          SerializableFunction<T, KV<String, WriteRequest>> writeItemMapperFn);

      abstract Write<T> build();
    }

    public Write<T> withAwsClientsProvider(AwsClientsProvider awsClientsProvider) {
      return builder().setAwsClientsProvider(awsClientsProvider).build();
    }

    public Write<T> withAwsClientsProvider(
        String awsAccessKey, String awsSecretKey, Regions region, String serviceEndpoint) {
      return withAwsClientsProvider(
          new BasicDynamoDBProvider(awsAccessKey, awsSecretKey, region, serviceEndpoint));
    }

    public Write<T> withAwsClientsProvider(
        String awsAccessKey, String awsSecretKey, Regions region) {
      return withAwsClientsProvider(awsAccessKey, awsSecretKey, region, null);
    }

    /**
     * Provides configuration to retry a failed request to publish a set of records to DynamoDB.
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
     *   .withRetryConfiguration(DynamoDBIO.RetryConfiguration.create(5, Duration.standardMinutes(1))
     *   ...
     * }</pre>
     *
     * @param retryConfiguration the rules which govern the retry behavior
     * @return the {@link DynamoDBIO.Write} with retrying configured
     */
    public Write<T> withRetryConfiguration(RetryConfiguration retryConfiguration) {
      checkArgument(retryConfiguration != null, "retryConfiguration is required");
      return builder().setRetryConfiguration(retryConfiguration).build();
    }

    public Write<T> withWriteRequestMapperFn(
        SerializableFunction<T, KV<String, WriteRequest>> writeItemMapperFn) {
      return builder().setWriteItemMapperFn(writeItemMapperFn).build();
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
      private transient AmazonDynamoDB client;
      private final DynamoDBIO.Write spec;
      private List<KV<String, WriteRequest>> batch;

      WriteFn(DynamoDBIO.Write spec) {
        this.spec = spec;
      }

      @Setup
      public void setup() {
        client = spec.getAwsClientsProvider().createDynamoDB();
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

          BatchWriteItemRequest batchRequest = new BatchWriteItemRequest();
          mapTableRequest
              .entrySet()
              .forEach(
                  entry -> batchRequest.addRequestItemsEntry(entry.getKey(), entry.getValue()));

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
                    "Unable to write batch items {}.",
                    batchRequest.getRequestItems().entrySet(),
                    ex);
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
          client.shutdown();
          client = null;
        }
      }
    }
  }
}
