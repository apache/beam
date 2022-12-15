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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.mapper;

import com.google.cloud.spanner.Dialect;
import java.io.Serializable;

/**
 * Factory class for creating instances that will map a struct to a connector model. The instances
 * created are all singletons.
 */
// transient fields are un-initialized, because we start them during the first fetch call (with the
// singleton pattern)
@SuppressWarnings({"initialization.fields.uninitialized", "initialization.field.uninitialized"})
public class MapperFactory implements Serializable {

  private static final long serialVersionUID = -813434573067800902L;

  private transient ChangeStreamRecordMapper changeStreamRecordMapperInstance;
  private transient PartitionMetadataMapper partitionMetadataMapperInstance;
  private final Dialect spannerChangeStreamDatabaseDialect;

  public MapperFactory(Dialect spannerChangeStreamDatabaseDialect) {
    this.spannerChangeStreamDatabaseDialect = spannerChangeStreamDatabaseDialect;
  }

  /**
   * Creates and returns a singleton instance of a mapper class capable of transforming a {@link
   * com.google.cloud.spanner.Struct} into a {@link java.util.List} of {@link
   * org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChangeStreamRecord} subclasses.
   *
   * <p>This method is thread safe.
   *
   * @return singleton instance of the {@link ChangeStreamRecordMapper}
   */
  public synchronized ChangeStreamRecordMapper changeStreamRecordMapper() {
    if (changeStreamRecordMapperInstance == null) {
      changeStreamRecordMapperInstance =
          new ChangeStreamRecordMapper(this.spannerChangeStreamDatabaseDialect);
    }
    return changeStreamRecordMapperInstance;
  }

  /**
   * Creates and returns a single instance of a mapper class capable of transforming a {@link
   * com.google.cloud.spanner.Struct} into a {@link
   * org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata} class.
   *
   * <p>This method is thread safe.
   *
   * @return singleton instance of the {@link PartitionMetadataMapper}
   */
  public synchronized PartitionMetadataMapper partitionMetadataMapper() {
    if (partitionMetadataMapperInstance == null) {
      partitionMetadataMapperInstance = new PartitionMetadataMapper();
    }
    return partitionMetadataMapperInstance;
  }
}
