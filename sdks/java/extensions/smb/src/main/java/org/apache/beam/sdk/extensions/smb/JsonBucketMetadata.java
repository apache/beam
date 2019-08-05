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
package org.apache.beam.sdk.extensions.smb;

import static org.apache.beam.sdk.coders.Coder.NonDeterministicException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;

/**
 * {@link org.apache.beam.sdk.extensions.smb.BucketMetadata} for BigQuery {@link TableRow} JSON
 * records.
 */
class JsonBucketMetadata<K> extends BucketMetadata<K, TableRow> {

  @JsonProperty private final String keyField;

  @JsonIgnore private String[] keyPath;

  public JsonBucketMetadata(
      int numBuckets,
      int numShards,
      Class<K> keyClass,
      BucketMetadata.HashType hashType,
      String keyField)
      throws CannotProvideCoderException, NonDeterministicException {
    this(BucketMetadata.CURRENT_VERSION, numBuckets, numShards, keyClass, hashType, keyField);
  }

  @JsonCreator
  JsonBucketMetadata(
      @JsonProperty("version") int version,
      @JsonProperty("numBuckets") int numBuckets,
      @JsonProperty("numShards") int numShards,
      @JsonProperty("keyClass") Class<K> keyClass,
      @JsonProperty("hashType") BucketMetadata.HashType hashType,
      @JsonProperty("keyField") String keyField)
      throws CannotProvideCoderException, NonDeterministicException {
    super(version, numBuckets, numShards, keyClass, hashType);
    this.keyField = keyField;
    this.keyPath = keyField.split("\\.");
  }

  @Override
  public K extractKey(TableRow value) {
    TableRow node = value;
    for (int i = 0; i < keyPath.length - 1; i++) {
      node = (TableRow) node.get(keyPath[i]);
    }
    @SuppressWarnings("unchecked")
    K key = (K) node.get(keyPath[keyPath.length - 1]);
    return key;
  }

  @Override
  public void populateDisplayData(Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("keyField", keyField));
  }
}
