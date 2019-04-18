package org.apache.beam.sdk.extensions.smb;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.vendor.guava.v20_0.com.google.common.hash.HashFunction;
import org.apache.beam.vendor.guava.v20_0.com.google.common.hash.Hashing;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

// @Todo = everything. This is just a placeholder for the kind of functionality we need
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = AvroBucketMetadata.class)
})
public abstract class BucketMetadata<SortingKeyT, ValueT> implements Serializable {

  @JsonProperty
  private final int numBuckets;

  @JsonProperty
  private final Class<SortingKeyT> sortingKeyClass;

  @JsonProperty
  private final HashType hashType;

  @JsonIgnore
  private final Coder<SortingKeyT> sortingKeyCoder;

  @JsonIgnore
  private final HashFunction hashFunction;

  public BucketMetadata(int numBuckets,
                        Class<SortingKeyT> sortingKeyClass,
                        HashType hashType) throws CannotProvideCoderException {
    this.numBuckets = numBuckets;
    this.sortingKeyClass = sortingKeyClass;
    this.hashType = hashType;

    this.sortingKeyCoder = CoderRegistry.createDefault().getCoder(sortingKeyClass);
    this.hashFunction = hashType.create();
  }

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

  ////////////////////////////////////////
  // Configuration
  ////////////////////////////////////////

  public int getNumBuckets() {
    return numBuckets;
  }

  public Class<SortingKeyT> getSortingKeyClass() {
    return sortingKeyClass;
  }

  public HashType getHashType() {
    return hashType;
  }

  public Coder<SortingKeyT> getSortingKeyCoder() {
    return sortingKeyCoder;
  }

  ////////////////////////////////////////
  // Business logic
  ////////////////////////////////////////

  // @todo use an actual hashing fn and modulo desired number of buckets
  public int assignBucket(ValueT value) throws CoderException {
    byte[] key = CoderUtils.encodeToByteArray(sortingKeyCoder, extractSortingKey(value));
    return hashFunction.hashBytes(key).asInt() % numBuckets;
  }

  public abstract SortingKeyT extractSortingKey(ValueT value);

  ////////////////////////////////////////
  // Serialization
  ////////////////////////////////////////

  @JsonIgnore
  private static ObjectMapper objectMapper = new ObjectMapper();

  public static <SortingKeyT, ValueT> BucketMetadata<SortingKeyT, ValueT> from(String src)
      throws IOException {
    return objectMapper.readerFor(BucketMetadata.class).readValue(src);
  }

  public static <SortingKeyT, ValueT> BucketMetadata<SortingKeyT, ValueT> from(InputStream src)
      throws IOException {
    return objectMapper.readerFor(BucketMetadata.class).readValue(src);
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
