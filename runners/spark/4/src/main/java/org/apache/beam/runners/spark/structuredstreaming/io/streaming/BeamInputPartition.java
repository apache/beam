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
package org.apache.beam.runners.spark.structuredstreaming.io.streaming;

import org.apache.spark.sql.connector.read.InputPartition;

/**
 * One split of a Beam unbounded source for one micro-batch.
 *
 * <p>Everything the executor needs travels as base64 of the Java serialized object, so the
 * partition works across JVMs and is not limited to Spark local mode.
 */
public class BeamInputPartition implements InputPartition {

  private static final long serialVersionUID = 1L;

  private final String sourceB64;
  private final String coderB64;
  private final String pipelineOptionsB64;
  private final String sourceId;
  private final int splitId;
  private final String checkpointLocation;
  private final long startEpoch;
  private final long endEpoch;
  private final int maxRecordsPerMicroBatch;
  private final long maxBatchDurationMillis;

  BeamInputPartition(
      String sourceB64,
      String coderB64,
      String pipelineOptionsB64,
      String sourceId,
      int splitId,
      String checkpointLocation,
      long startEpoch,
      long endEpoch,
      int maxRecordsPerMicroBatch,
      long maxBatchDurationMillis) {
    this.sourceB64 = sourceB64;
    this.coderB64 = coderB64;
    this.pipelineOptionsB64 = pipelineOptionsB64;
    this.sourceId = sourceId;
    this.splitId = splitId;
    this.checkpointLocation = checkpointLocation;
    this.startEpoch = startEpoch;
    this.endEpoch = endEpoch;
    this.maxRecordsPerMicroBatch = maxRecordsPerMicroBatch;
    this.maxBatchDurationMillis = maxBatchDurationMillis;
  }

  String sourceB64() {
    return sourceB64;
  }

  String coderB64() {
    return coderB64;
  }

  String pipelineOptionsB64() {
    return pipelineOptionsB64;
  }

  String sourceId() {
    return sourceId;
  }

  int splitId() {
    return splitId;
  }

  String checkpointLocation() {
    return checkpointLocation;
  }

  long startEpoch() {
    return startEpoch;
  }

  long endEpoch() {
    return endEpoch;
  }

  int maxRecordsPerMicroBatch() {
    return maxRecordsPerMicroBatch;
  }

  long maxBatchDurationMillis() {
    return maxBatchDurationMillis;
  }

  @Override
  public String toString() {
    return "BeamInputPartition{source=" + sourceId + ", split=" + splitId + "}";
  }
}
