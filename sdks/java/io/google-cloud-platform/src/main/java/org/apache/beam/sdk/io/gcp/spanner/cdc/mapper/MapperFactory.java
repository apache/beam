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
package org.apache.beam.sdk.io.gcp.spanner.cdc.mapper;

import java.io.Serializable;

// TODO: Add java docs
public class MapperFactory implements Serializable {

  private static final long serialVersionUID = -813434573067800902L;
  private static ChangeStreamRecordMapper changeStreamRecordMapperInstance;
  private static PartitionMetadataMapper partitionMetadataMapperInstance;

  // TODO: See if synchronized is a bottleneck and refactor if so
  public synchronized ChangeStreamRecordMapper changeStreamRecordMapper() {
    if (changeStreamRecordMapperInstance == null) {
      changeStreamRecordMapperInstance = new ChangeStreamRecordMapper();
    }
    return changeStreamRecordMapperInstance;
  }

  public synchronized PartitionMetadataMapper partitionMetadataMapper() {
    if (partitionMetadataMapperInstance == null) {
      partitionMetadataMapperInstance = new PartitionMetadataMapper();
    }
    return partitionMetadataMapperInstance;
  }
}
