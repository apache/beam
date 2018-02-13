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

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.producer.IKinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import java.io.Serializable;

/**
 * Provides instances of AWS clients.
 *
 * <p>Please note, that any instance of {@link AWSClientsProvider} must be {@link Serializable} to
 * ensure it can be sent to worker machines.
 */
public interface AWSClientsProvider extends Serializable {
  AmazonKinesis getKinesisClient();

  AmazonCloudWatch getCloudWatchClient();

  IKinesisProducer createKinesisProducer(KinesisProducerConfiguration config);
}
