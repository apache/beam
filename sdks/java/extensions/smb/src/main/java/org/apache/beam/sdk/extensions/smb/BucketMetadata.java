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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.smb.avro.AvroBucketMetadata;
import org.apache.beam.vendor.guava.v20_0.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v20_0.com.google.common.hash.HashFunction;
import org.apache.beam.vendor.guava.v20_0.com.google.common.hash.Hashing;

/**
 * Represents SMB metadata in a JSON-serializable format to be stored along with bucketed data.
 *
 * @param <KeyT>
 * @param <ValueT>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({@JsonSubTypes.Type(value = AvroBucketMetadata.class)})
public abstract class BucketMetadata<KeyT, ValueT> implements Serializable {

  @JsonProperty private final int numBuckets;

  @JsonProperty private final Class<KeyT> sortingKeyClass;

  @JsonProperty private final HashType hashType;

  @JsonIgnore private final HashFunction hashFunction;

  @JsonIgnore private final Coder<KeyT> keyCoder;

  public BucketMetadata(int numBuckets, Class<KeyT> sortingKeyClass, HashType hashType)
      throws CannotProvideCoderException {
    this.numBuckets = numBuckets;
    this.sortingKeyClass = sortingKeyClass;
    this.hashType = hashType;
    this.hashFunction = hashType.create();

    this.keyCoder = getSortingKeyCoder();
  }

  @JsonIgnore
  public abstract Coder<KeyT> getSortingKeyCoder() throws CannotProvideCoderException;

  /** Enumerated hashing schemes available for an SMB sink. */
  public enum HashType {
    MURMUR3_32 {
      @Override
      public HashFunction create() {
        return Hashing.murmur3_32();
      }
    },
    MURMUR3_128 {
      @Override
      public HashFunction create() {
        return Hashing.murmur3_128();
      }
    };

    public abstract HashFunction create();
  }

  // Todo: more sophisticated comparison rules.
  @VisibleForTesting
  public boolean compatibleWith(BucketMetadata other) {
    if (other == null) {
      return false;
    }

    boolean bucketsCompatible =
        (this.numBuckets == other.numBuckets)
            || Math.max(this.numBuckets, other.numBuckets)
                    % Math.min(this.numBuckets, other.numBuckets)
                == 0;

    return bucketsCompatible
        && this.hashType == other.hashType
        && this.sortingKeyClass == other.sortingKeyClass;
  }

  ////////////////////////////////////////
  // Configuration
  ////////////////////////////////////////

  public HashFunction getHashFunction() {
    return hashFunction;
  }

  public int getNumBuckets() {
    return numBuckets;
  }

  public Class<KeyT> getSortingKeyClass() {
    return sortingKeyClass;
  }

  public HashType getHashType() {
    return hashType;
  }

  ////////////////////////////////////////
  // Business logic
  ////////////////////////////////////////

  byte[] extractKeyBytes(ValueT value) {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    KeyT key = extractKey(value);
    try {
      keyCoder.encode(key, baos);
    } catch (Exception e) {
      throw new RuntimeException("Could not encode key " + key, e);
    }

    return baos.toByteArray();
  }

  public abstract KeyT extractKey(ValueT value);

  ////////////////////////////////////////
  // Serialization
  ////////////////////////////////////////

  @JsonIgnore private static ObjectMapper objectMapper = new ObjectMapper();

  @VisibleForTesting
  public static <KeyT, ValueT> BucketMetadata<KeyT, ValueT> from(String src) throws IOException {
    return objectMapper.readerFor(BucketMetadata.class).readValue(src);
  }

  @VisibleForTesting
  public static <KeyT, ValueT> BucketMetadata<KeyT, ValueT> from(InputStream src)
      throws IOException {
    return objectMapper.readerFor(BucketMetadata.class).readValue(src);
  }

  @VisibleForTesting
  public static <KeyT, ValueT> void to(
      BucketMetadata<KeyT, ValueT> bucketMetadata, OutputStream outputStream) throws IOException {

    objectMapper.writeValue(outputStream, bucketMetadata);
  }

  @Override
  public String toString() {
    try {
      return objectMapper.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
