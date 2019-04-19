package org.apache.beam.sdk.extensions.smb.avro;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.extensions.smb.BucketMetadata;

public class AvroBucketMetadata<SortingKeyT> extends BucketMetadata<SortingKeyT, GenericRecord> {

  @JsonProperty
  private final String keyField;

  @JsonIgnore
  private String[] keyPath;

  @JsonCreator
  public AvroBucketMetadata(@JsonProperty("numBuckets") int numBuckets,
                            @JsonProperty("sortingKeyClass") Class<SortingKeyT> sortingKeyClass,
                            @JsonProperty("hashType") HashType hashType,
                            @JsonProperty("keyField") String keyField)
          throws CannotProvideCoderException {
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
