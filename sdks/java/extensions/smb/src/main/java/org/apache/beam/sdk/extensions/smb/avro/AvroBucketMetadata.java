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
package org.apache.beam.sdk.extensions.smb.avro;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.beam.sdk.extensions.smb.BucketMetadata;

/** Avro-specific metadata encoding. */
public class AvroBucketMetadata {

  /**
   * Represents a metadata encoding for GenericRecords.
   *
   * @param <SortingKeyT>
   */
  public static class GenericRecordMetadata<SortingKeyT>
      extends BucketMetadata<SortingKeyT, GenericRecord> {

    @JsonProperty private final String keyField;

    @JsonIgnore private String[] keyPath;

    @JsonCreator
    public GenericRecordMetadata(
        @JsonProperty("numBuckets") int numBuckets,
        @JsonProperty("sortingKeyClass") Class<SortingKeyT> sortingKeyClass,
        @JsonProperty("hashType") BucketMetadata.HashType hashType,
        @JsonProperty("keyField") String keyField) {
      super(numBuckets, sortingKeyClass, hashType);
      this.keyField = keyField;
      this.keyPath = keyField.split("\\.");
    }

    @SuppressWarnings("unchecked")
    @Override
    public SortingKeyT extractSortingKey(GenericRecord value) {
      GenericRecord node = value;
      for (int i = 0; i < keyPath.length - 1; i++) {
        node = (GenericRecord) node.get(keyPath[i]);
      }
      return (SortingKeyT) node.get(keyPath[keyPath.length - 1]);
    }
  }

  /**
   * Represents a metadata encoding for extensions of SpecificRecordBase.
   *
   * @param <SortingKeyT>
   * @param <AvroTypeT>
   */
  public static class SpecificRecordMetadata<SortingKeyT, AvroTypeT extends SpecificRecordBase>
      extends BucketMetadata<SortingKeyT, AvroTypeT> {

    @JsonProperty private final String keyField;

    @JsonIgnore private String[] keyPath;

    @JsonCreator
    public SpecificRecordMetadata(
        @JsonProperty("numBuckets") int numBuckets,
        @JsonProperty("sortingKeyClass") Class<SortingKeyT> sortingKeyClass,
        @JsonProperty("hashType") BucketMetadata.HashType hashType,
        @JsonProperty("keyField") String keyField) {
      super(numBuckets, sortingKeyClass, hashType);
      this.keyField = keyField;
      this.keyPath = keyField.split("\\.");
    }

    @SuppressWarnings("unchecked")
    @Override
    public SortingKeyT extractSortingKey(AvroTypeT value) {
      SpecificRecordBase node = value;
      for (int i = 0; i < keyPath.length - 1; i++) {
        node = (SpecificRecordBase) node.get(keyPath[i]);
      }
      return (SortingKeyT) node.get(keyPath[keyPath.length - 1]);
    }
  }
}
