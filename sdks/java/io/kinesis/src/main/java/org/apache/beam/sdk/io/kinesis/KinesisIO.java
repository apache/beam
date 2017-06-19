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
package org.apache.beam.sdk.io.kinesis;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.google.auto.value.AutoValue;

import javax.annotation.Nullable;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.io.BoundedReadFromUnboundedSource;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * {@link PTransform}s for reading from
 * <a href="https://aws.amazon.com/kinesis/">Kinesis</a> streams.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * p.apply(KinesisIO.read()
 *     .from("streamName", InitialPositionInStream.LATEST)
 *     .withClientProvider("AWS_KEY", _"AWS_SECRET", STREAM_REGION)
 *  .apply( ... ) // other transformations
 * }</pre>
 *
 * <p>As you can see you need to provide 3 things:
 * <ul>
 *   <li>name of the stream you're going to read</li>
 *   <li>position in the stream where reading should start. There are two options:
 *   <ul>
 *     <li>{@link InitialPositionInStream#LATEST} - reading will begin from end of the stream</li>
 *     <li>{@link InitialPositionInStream#TRIM_HORIZON} - reading will begin at
 *        the very beginning of the stream</li>
 *   </ul></li>
 *   <li>data used to initialize {@link AmazonKinesis} client:
 *   <ul>
 *     <li>credentials (aws key, aws secret)</li>
 *    <li>region where the stream is located</li>
 *   </ul></li>
 * </ul>
 *
 * <p>In case when you want to set up {@link AmazonKinesis} client by your own
 * (for example if you're using more sophisticated authorization methods like Amazon STS, etc.)
 * you can do it by implementing {@link KinesisClientProvider} class:
 *
 * <pre>{@code
 * public class MyCustomKinesisClientProvider implements KinesisClientProvider {
 *   {@literal @}Override
 *   public AmazonKinesis get() {
 *     // set up your client here
 *   }
 * }
 * }</pre>
 *
 * <p>Usage is pretty straightforward:
 *
 * <pre>{@code
 * p.apply(KinesisIO.read()
 *    .from("streamName", InitialPositionInStream.LATEST)
 *    .withClientProvider(new MyCustomKinesisClientProvider())
 *  .apply( ... ) // other transformations
 * }</pre>
 *
 * <p>There’s also possibility to start reading using arbitrary point in time -
 * in this case you need to provide {@link Instant} object:
 *
 * <pre>{@code
 * p.apply(KinesisIO.read()
 *     .from("streamName", instant)
 *     .withClientProvider(new MyCustomKinesisClientProvider())
 *  .apply( ... ) // other transformations
 * }</pre>
 *
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public final class KinesisIO {

  /** Returns a new {@link Read} transform for reading from Kinesis. */
  public static Read read() {
    return new AutoValue_KinesisIO_Read.Builder().setMaxNumRecords(-1).build();
  }

  /** Implementation of {@link #read}. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<KinesisRecord>> {

    @Nullable
    abstract String getStreamName();

    @Nullable
    abstract StartingPoint getInitialPosition();

    @Nullable
    abstract KinesisClientProvider getClientProvider();

    abstract int getMaxNumRecords();

    @Nullable
    abstract Duration getMaxReadTime();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setStreamName(String streamName);

      abstract Builder setInitialPosition(StartingPoint startingPoint);

      abstract Builder setClientProvider(KinesisClientProvider clientProvider);

      abstract Builder setMaxNumRecords(int maxNumRecords);

      abstract Builder setMaxReadTime(Duration maxReadTime);

      abstract Read build();
    }

    /**
     * Specify reading from streamName at some initial position.
     */
    public Read from(String streamName, InitialPositionInStream initialPosition) {
      return toBuilder()
          .setStreamName(streamName)
          .setInitialPosition(
              new StartingPoint(checkNotNull(initialPosition, "initialPosition")))
          .build();
    }

    /**
     * Specify reading from streamName beginning at given {@link Instant}.
     * This {@link Instant} must be in the past, i.e. before {@link Instant#now()}.
     */
    public Read from(String streamName, Instant initialTimestamp) {
      return toBuilder()
          .setStreamName(streamName)
          .setInitialPosition(
              new StartingPoint(checkNotNull(initialTimestamp, "initialTimestamp")))
          .build();
    }

    /**
     * Allows to specify custom {@link KinesisClientProvider}.
     * {@link KinesisClientProvider} provides {@link AmazonKinesis} instances which are later
     * used for communication with Kinesis.
     * You should use this method if {@link Read#withClientProvider(String, String, Regions)}
     * does not suit your needs.
     */
    public Read withClientProvider(KinesisClientProvider kinesisClientProvider) {
      return toBuilder().setClientProvider(kinesisClientProvider).build();
    }

    /**
     * Specify credential details and region to be used to read from Kinesis.
     * If you need more sophisticated credential protocol, then you should look at
     * {@link Read#withClientProvider(KinesisClientProvider)}.
     */
    public Read withClientProvider(String awsAccessKey, String awsSecretKey, Regions region) {
      return withClientProvider(new BasicKinesisProvider(awsAccessKey, awsSecretKey, region));
    }

    /** Specifies to read at most a given number of records. */
    public Read withMaxNumRecords(int maxNumRecords) {
      checkArgument(
          maxNumRecords > 0, "maxNumRecords must be positive, but was: %s", maxNumRecords);
      return toBuilder().setMaxNumRecords(maxNumRecords).build();
    }

    /** Specifies to read at most a given number of records. */
    public Read withMaxReadTime(Duration maxReadTime) {
      checkNotNull(maxReadTime, "maxReadTime");
      return toBuilder().setMaxReadTime(maxReadTime).build();
    }

    @Override
    public PCollection<KinesisRecord> expand(PBegin input) {
      org.apache.beam.sdk.io.Read.Unbounded<KinesisRecord> read =
          org.apache.beam.sdk.io.Read.from(
              new KinesisSource(getClientProvider(), getStreamName(), getInitialPosition()));
      if (getMaxNumRecords() > 0) {
        BoundedReadFromUnboundedSource<KinesisRecord> bounded =
            read.withMaxNumRecords(getMaxNumRecords());
        return getMaxReadTime() == null
            ? input.apply(bounded)
            : input.apply(bounded.withMaxReadTime(getMaxReadTime()));
      } else {
        return getMaxReadTime() == null
            ? input.apply(read)
            : input.apply(read.withMaxReadTime(getMaxReadTime()));
      }
    }

    private static final class BasicKinesisProvider implements KinesisClientProvider {

      private final String accessKey;
      private final String secretKey;
      private final Regions region;

      private BasicKinesisProvider(String accessKey, String secretKey, Regions region) {
        this.accessKey = checkNotNull(accessKey, "accessKey");
        this.secretKey = checkNotNull(secretKey, "secretKey");
        this.region = checkNotNull(region, "region");
      }

      private AWSCredentialsProvider getCredentialsProvider() {
        return new StaticCredentialsProvider(new BasicAWSCredentials(
            accessKey,
            secretKey
        ));

      }

      @Override
      public AmazonKinesis get() {
        AmazonKinesisClient client = new AmazonKinesisClient(getCredentialsProvider());
        client.withRegion(region);
        return client;
      }
    }
  }
}
