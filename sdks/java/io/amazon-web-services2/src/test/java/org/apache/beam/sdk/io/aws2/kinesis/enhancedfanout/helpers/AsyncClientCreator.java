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
package org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.helpers;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;

public class AsyncClientCreator {
  private final KinesisClientStubConfig config;
  private final Function<KinesisClientStubShardState, Void> eventsSubmitter;
  private final BlockingQueue<SubscribeToShardRequest> subscribeRequestsSeen =
      new LinkedBlockingQueue<>(Integer.MAX_VALUE);
  private final AtomicInteger seqNumber = new AtomicInteger();

  AsyncClientCreator(
      KinesisClientStubConfig config, Function<KinesisClientStubShardState, Void> eventsSubmitter) {
    this.config = config;
    this.eventsSubmitter = eventsSubmitter;
  }

  public KinesisClientProxyStub create() {
    return new KinesisClientProxyStub(
        config,
        new AtomicInteger(config.getSubscriptionsPerShard()),
        seqNumber,
        subscribeRequestsSeen,
        eventsSubmitter);
  }
}
