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

public class KinesisStubBehaviours {
  public static KinesisClientProxyStub twoShardsWithRecords() {
    KinesisClientStubConfig config = new KinesisClientStubConfig(2, 1, 4, 3);
    return new AsyncClientCreator(config, KinesisClientStubShardState::submitEventsAndThenComplete)
        .create();
  }

  public static KinesisClientProxyStub twoShardsWithRecordsAndShardUp() {
    KinesisClientStubConfig config = new KinesisClientStubConfig(2, 5, 10, 7);
    return new AsyncClientCreator(config, KinesisClientStubShardState::submitEventsAndThenShardUp)
        .create();
  }

  public static KinesisClientProxyStub fourShardsWithRecordsAndShardDown() {
    KinesisClientStubConfig config = new KinesisClientStubConfig(4, 6, 7, 11);
    return new AsyncClientCreator(config, KinesisClientStubShardState::submitEventsAndThenShardDown)
        .create();
  }

  public static KinesisClientProxyStub twoShardsEmpty() {
    KinesisClientStubConfig config = new KinesisClientStubConfig(2, 1, 2, 0);
    return new AsyncClientCreator(config, KinesisClientStubShardState::submitEventsAndThenComplete)
        .create();
  }

  public static KinesisClientProxyStub twoShardsWithRecordsOneShardError() {
    KinesisClientStubConfig config = new KinesisClientStubConfig(2, 1, 4, 5);
    return new AsyncClientCreator(config, KinesisClientStubShardState::submitEventsAndThenSendError)
        .create();
  }

  public static KinesisClientProxyStub twoShardsWithRecordsOneShardRecoverableError() {
    KinesisClientStubConfig config = new KinesisClientStubConfig(2, 1, 4, 5);
    return new AsyncClientCreator(
            config, KinesisClientStubShardState::submitEventsAndThenSendRecoverableError)
        .create();
  }
}
