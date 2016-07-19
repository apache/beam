package org.apache.beam.sdk.io.kinesis;
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

import org.apache.beam.sdk.io.kinesis.client.KinesisClientProvider;
import org.apache.beam.sdk.io.kinesis.client.response.KinesisRecord;
import org.apache.beam.sdk.io.kinesis.source.KinesisSource;
import org.apache.beam.sdk.io.kinesis.source.checkpoint.StartingPoint;
import org.apache.beam.sdk.transforms.PTransform;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import java.util.Date;

/**
 * {@link PTransform}s for reading from
 * <a href="https://aws.amazon.com/kinesis/">Kinesis</a> streams.
 */
public class KinesisIO {
    /***
     * A {@link PTransform} that reads from a Kinesis stream.
     */
    public static class Read {

        private final String streamName;
        private final StartingPoint initialPosition;

        private Read(String streamName, StartingPoint initialPosition) {
            this.streamName = streamName;
            this.initialPosition = initialPosition;
        }

        /***
         * Specify reading from streamName at some initial position.
         */
        public static Read from(String streamName, InitialPositionInStream initialPosition) {
            return new Read(streamName, new StartingPoint(initialPosition));
        }

        public static Read from(String streamName, Date initialTimestamp) {
            return new Read(streamName, new StartingPoint(initialTimestamp));
        }

        /***
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

        /***
         * Specify credential details and region to be used to read from Kinesis.
         * If you need more sophisticated credential protocol, then you should look at
         * {@link Read#using(KinesisClientProvider)}.
         */
        public org.apache.beam.sdk.io.Read.Unbounded<KinesisRecord> using(String awsAccessKey,
                                                                          String awsSecretKey,
                                                                          Regions region) {
            return using(new BasicKinesisProvider(awsAccessKey, awsSecretKey, region));
        }

        private static class BasicKinesisProvider implements KinesisClientProvider {

            private final String accessKey;
            private final String secretKey;
            private final Regions region;

            private BasicKinesisProvider(String accessKey, String secretKey, Regions region) {
                this.accessKey = accessKey;
                this.secretKey = secretKey;
                this.region = region;
            }


            private AWSCredentialsProvider getCredentialsProvider() {
                return new StaticCredentialsProvider(new BasicAWSCredentials(
                        accessKey,
                        secretKey
                ));

            }

            @Override
            public AmazonKinesis get() {
                return new AmazonKinesisClient(getCredentialsProvider()).withRegion(region);
            }
        }
    }
}
