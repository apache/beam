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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.tensorflow.example.BytesList;
import org.tensorflow.example.Example;
import org.tensorflow.example.Feature;
import org.tensorflow.example.FloatList;
import org.tensorflow.example.Int64List;

/** {@link BucketMetadata} for TensorFlow {@link Example} records. */
class TensorFlowBucketMetadata<K> extends BucketMetadata<K, Example> {

  @JsonProperty private final String keyField;

  public TensorFlowBucketMetadata(
      int numBuckets,
      int numShards,
      Class<K> keyClass,
      BucketMetadata.HashType hashType,
      String keyField)
      throws CannotProvideCoderException, NonDeterministicException {
    this(BucketMetadata.CURRENT_VERSION, numBuckets, numShards, keyClass, hashType, keyField);
  }

  @JsonCreator
  TensorFlowBucketMetadata(
      @JsonProperty("version") int version,
      @JsonProperty("numBuckets") int numBuckets,
      @JsonProperty("numShards") int numShards,
      @JsonProperty("keyClass") Class<K> keyClass,
      @JsonProperty("hashType") BucketMetadata.HashType hashType,
      @JsonProperty("keyField") String keyField)
      throws CannotProvideCoderException, NonDeterministicException {
    super(version, numBuckets, numShards, keyClass, hashType);
    this.keyField = keyField;
  }

  @SuppressWarnings("unchecked")
  @Override
  public K extractKey(Example value) {
    Feature feature = value.getFeatures().getFeatureMap().get(keyField);
    if (getKeyClass() == byte[].class) {
      BytesList values = feature.getBytesList();
      Preconditions.checkState(values.getValueCount() == 1, "Number of feature in keyField != 1");
      return (K) values.getValue(0).toByteArray();
    } else if (getKeyClass() == Long.class) {
      Int64List values = feature.getInt64List();
      Preconditions.checkState(values.getValueCount() == 1, "Number of feature in keyField != 1");
      return (K) Long.valueOf(values.getValue(0));
    } else if (getKeyClass() == Float.class) {
      FloatList values = feature.getFloatList();
      Preconditions.checkState(values.getValueCount() == 1, "Number of feature in keyField != 1");
      return (K) Float.valueOf(values.getValue(0));
    }
    throw new IllegalStateException("Unsupported key class " + getKeyClass());
  }

  @Override
  public void populateDisplayData(Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("keyField", keyField));
  }
}
