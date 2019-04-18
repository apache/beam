package org.apache.beam.sdk.extensions.smb;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.CannotProvideCoderException;

import java.io.IOException;

public class Test {
  public static void main(String[] args) throws IOException, CannotProvideCoderException {
    BucketMetadata<String, GenericRecord> metadata = new AvroBucketMetadata<>(
            10, String.class, BucketMetadata.HashType.MURMUR3_32, "a.b.c");
    System.out.println("==========");
    System.out.println(metadata);

    metadata = BucketMetadata.from(metadata.toString());
    System.out.println("==========");
    System.out.println(metadata.getNumBuckets());
    System.out.println(metadata.getSortingKeyClass());
    System.out.println(metadata.getHashType());
    System.out.println(metadata.getSortingKeyCoder());
  }

}
