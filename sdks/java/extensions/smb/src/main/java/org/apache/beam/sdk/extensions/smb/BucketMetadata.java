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
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.vendor.guava.v20_0.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v20_0.com.google.common.hash.HashFunction;
import org.apache.beam.vendor.guava.v20_0.com.google.common.hash.Hashing;

/**
 * Represents metadata in a JSON-serializable format to be stored alongside sorted-bucket files.
 *
 * @param <K> the type of the keys that values in a bucket are sorted with
 * @param <V> the type of the values in a bucket
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "type")
public abstract class BucketMetadata<K, V> implements Serializable {

  @JsonIgnore public static final int CURRENT_VERSION = 0;

  // Represents the current major version of the Beam SMB module. Storage format may differ
  // across versions and require internal code branching to ensure backwards compatibility.
  @JsonProperty private final int version;

  @JsonProperty private final int numBuckets;

  @JsonProperty private final int numShards;

  @JsonProperty private final Class<K> keyClass;

  @JsonProperty private final HashType hashType;

  @JsonIgnore private final HashFunction hashFunction;

  @JsonIgnore private final Coder<K> keyCoder;

  public BucketMetadata(
      int version, int numBuckets, int numShards, Class<K> keyClass, HashType hashType)
      throws CannotProvideCoderException, NonDeterministicException {
    Preconditions.checkArgument(
        numBuckets > 0 && ((numBuckets & (numBuckets - 1)) == 0),
        "numBuckets must be a power of 2");
    Preconditions.checkArgument(numShards > 0, "numShards must be > 0");

    this.numBuckets = numBuckets;
    this.numShards = numShards;
    this.keyClass = keyClass;
    this.hashType = hashType;
    this.hashFunction = hashType.create();
    this.keyCoder = getKeyCoder();
    this.version = version;
  }

  @JsonIgnore
  Coder<K> getKeyCoder() throws CannotProvideCoderException, NonDeterministicException {
    @SuppressWarnings("unchecked")
    Coder<K> coder = (Coder<K>) coderOverrides().get(getKeyClass());
    if (coder == null) {
      coder = CoderRegistry.createDefault().getCoder(keyClass);
    }
    coder.verifyDeterministic();
    return coder;
  }

  @JsonIgnore
  protected Map<Class<?>, Coder<?>> coderOverrides() {
    return Collections.emptyMap();
  }

  /** Enumerated hashing schemes available for an SMB write. */
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

  boolean isCompatibleWith(BucketMetadata other) {
    return other != null
        && this.version == other.version
        && this.hashType == other.hashType
        // This check should be redundant since power of two is checked in BucketMetadata
        // constructor, but it's cheap to double-check.
        && (Math.max(numBuckets, other.numBuckets) % Math.min(numBuckets, other.numBuckets) == 0);
  }

  ////////////////////////////////////////
  // Configuration
  ////////////////////////////////////////

  public int getVersion() {
    return version;
  }

  public int getNumBuckets() {
    return numBuckets;
  }

  public int getNumShards() {
    return numShards;
  }

  public Class<K> getKeyClass() {
    return keyClass;
  }

  public HashType getHashType() {
    return hashType;
  }

  ////////////////////////////////////////
  // Business logic
  ////////////////////////////////////////

  byte[] getKeyBytes(V value) {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final K key = extractKey(value);
    try {
      keyCoder.encode(key, baos);
    } catch (Exception e) {
      throw new RuntimeException("Could not encode key " + key, e);
    }

    return baos.toByteArray();
  }

  public abstract K extractKey(V value);

  int getBucketId(byte[] keyBytes) {
    return Math.abs(hashFunction.hashBytes(keyBytes).asInt()) % numBuckets;
  }

  ////////////////////////////////////////
  // Serialization
  ////////////////////////////////////////

  @JsonIgnore private static ObjectMapper objectMapper = new ObjectMapper();

  @VisibleForTesting
  public static <K, V> BucketMetadata<K, V> from(String src) throws IOException {
    return objectMapper.readerFor(BucketMetadata.class).readValue(src);
  }

  @VisibleForTesting
  public static <K, V> BucketMetadata<K, V> from(InputStream src) throws IOException {
    return objectMapper.readerFor(BucketMetadata.class).readValue(src);
  }

  @VisibleForTesting
  public static <K, V> void to(BucketMetadata<K, V> bucketMetadata, OutputStream outputStream)
      throws IOException {

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

  static class BucketMetadataCoder<K, V> extends AtomicCoder<BucketMetadata<K, V>> {
    private static final StringUtf8Coder stringCoder = StringUtf8Coder.of();

    @Override
    public void encode(BucketMetadata<K, V> value, OutputStream outStream)
        throws CoderException, IOException {
      stringCoder.encode(value.toString(), outStream);
    }

    @Override
    public BucketMetadata<K, V> decode(InputStream inStream) throws CoderException, IOException {
      return BucketMetadata.from(stringCoder.decode(inStream));
    }
  }
}
