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

class TestBucketMetadata extends BucketMetadata<String, String> {

  static TestBucketMetadata of(int numBuckets, int numShards)
      throws CannotProvideCoderException, NonDeterministicException {
    return new TestBucketMetadata(numBuckets, numShards, HashType.MURMUR3_32);
  }

  TestBucketMetadata(
      @JsonProperty("numBuckets") int numBuckets,
      @JsonProperty("numShards") int numShards,
      @JsonProperty("hashType") HashType hashType)
      throws CannotProvideCoderException, NonDeterministicException {
    this(BucketMetadata.CURRENT_VERSION, numBuckets, numShards, hashType);
  }

  @JsonCreator
  TestBucketMetadata(
      @JsonProperty("version") int version,
      @JsonProperty("numBuckets") int numBuckets,
      @JsonProperty("numShards") int numShards,
      @JsonProperty("hashType") HashType hashType)
      throws CannotProvideCoderException, NonDeterministicException {
    super(version, numBuckets, numShards, String.class, hashType);
  }

  @Override
  public String extractKey(String value) {
    return value.substring(0, 1);
  }
}
