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


import static com.google.common.base.Preconditions.checkNotNull;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.PTransform;
import org.joda.time.Instant;

/**
 * {@link PTransform}s for reading from
 * <a href="https://aws.amazon.com/kinesis/">Kinesis</a> streams.
 *
 * <h3>Usage</h3>
 *
 * <p>Main class you're going to operate is called {@link KinesisIO}.
 * It follows the usage conventions laid out by other *IO classes like
 * BigQueryIO or PubsubIOLet's see how you can set up a simple Pipeline, which reads from Kinesis:
 *
 * <pre>{@code
 * p.
 *   apply(KinesisIO.Read.
 *     from("streamName", InitialPositionInStream.LATEST).
 *     using("AWS_KEY", _"AWS_SECRET", STREAM_REGION).
 *     apply( ... ) // other transformations
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
 * p.
 *   apply(KinesisIO.Read.
 *    from("streamName", InitialPositionInStream.LATEST).
 *    using(MyCustomKinesisClientProvider()).
 *    apply( ... ) // other transformations
 * }</pre>
 *
 * <p>There’s also possibility to start reading using arbitrary point in time -
 * in this case you need to provide {@link Instant} object:
 *
 * <pre>{@code
 * p.
 *   apply(KinesisIO.Read.
 *     from("streamName", instant).
 *     using(MyCustomKinesisClientProvider()).
 *     apply( ... ) // other transformations
 * }</pre>
 *
 */
@Experimental
public final class KinesisIO {
    /**
     * A {@link PTransform} that reads from a Kinesis stream.
     */
    public static final class Read {

        private final String streamName;
        private final StartingPoint initialPosition;

        private Read(String streamName, StartingPoint initialPosition) {
            this.streamName = checkNotNull(streamName, "streamName");
            this.initialPosition = checkNotNull(initialPosition, "initialPosition");
        }

        /**
         * Specify reading from streamName at some initial position.
         */
        public static Read from(String streamName, InitialPositionInStream initialPosition) {
            return new Read(streamName, new StartingPoint(
                    checkNotNull(initialPosition, "initialPosition")));
        }

        /**
         * Specify reading from streamName beginning at given {@link Instant}.
         * This {@link Instant} must be in the past, i.e. before {@link Instant#now()}.
         */
        public static Read from(String streamName, Instant initialTimestamp) {
            return new Read(streamName, new StartingPoint(
                    checkNotNull(initialTimestamp, "initialTimestamp")));
        }

        /**
         * Allows to specify custom {@link KinesisClientProvider}.
         * {@link KinesisClientProvider} provides {@link AmazonKinesis} instances which are later
         * used for communication with Kinesis.
         * You should use this method if {@link Read#using(String, String, Regions)} does not
         * suite your needs.
         */
        public org.apache.beam.sdk.io.Read.Unbounded<KinesisRecord> using
        (KinesisClientProvider kinesisClientProvider) {
            return org.apache.beam.sdk.io.Read.from(
                    new KinesisSource(kinesisClientProvider, streamName,
                            initialPosition));
        }

        /**
         * Specify credential details and region to be used to read from Kinesis.
         * If you need more sophisticated credential protocol, then you should look at
         * {@link Read#using(KinesisClientProvider)}.
         */
        public org.apache.beam.sdk.io.Read.Unbounded<KinesisRecord> using(String awsAccessKey,
                                                                          String awsSecretKey,
                                                                          Regions region) {
            return using(new BasicKinesisProvider(awsAccessKey, awsSecretKey, region));
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
