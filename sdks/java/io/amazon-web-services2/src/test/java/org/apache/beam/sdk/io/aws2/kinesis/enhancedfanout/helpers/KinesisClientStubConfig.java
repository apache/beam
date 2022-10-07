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

class KinesisClientStubConfig {
  private final Integer initialShardsCnt;
  private final Integer finalShardId;
  private final Integer subscriptionsCnt;
  private final Integer recordsPerSubscriptionPerShardToSend;

  KinesisClientStubConfig(
      Integer initialShardsCnt,
      Integer finalShardId,
      Integer subscriptionsCnt,
      Integer recordsPerSubscriptionPerShardToSend) {
    this.initialShardsCnt = initialShardsCnt;
    this.finalShardId = finalShardId;
    this.subscriptionsCnt = subscriptionsCnt;
    this.recordsPerSubscriptionPerShardToSend = recordsPerSubscriptionPerShardToSend;
  }

  public Integer getInitialShardsCnt() {
    return initialShardsCnt;
  }

  public Integer getFinalShardId() {
    return finalShardId;
  }

  public Integer getSubscriptionsPerShard() {
    return subscriptionsCnt;
  }

  public Integer getRecordsPerSubscriptionPerShardToSend() {
    return recordsPerSubscriptionPerShardToSend;
  }
}
