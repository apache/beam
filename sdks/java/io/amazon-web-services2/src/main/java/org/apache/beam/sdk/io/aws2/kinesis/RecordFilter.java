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
package org.apache.beam.sdk.io.aws2.kinesis;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists.newArrayList;

import java.util.List;

/**
 * Filters out records, which were already processed and checkpointed.
 *
 * <p>We need this step, because we can get iterators from Kinesis only with "sequenceNumber"
 * accuracy, not with "subSequenceNumber" accuracy.
 */
class RecordFilter {

  public List<KinesisRecord> apply(List<KinesisRecord> records, ShardCheckpoint checkpoint) {
    List<KinesisRecord> filteredRecords = newArrayList();
    for (KinesisRecord record : records) {
      if (checkpoint.isBeforeOrAt(record)) {
        filteredRecords.add(record);
      }
    }
    return filteredRecords;
  }
}
