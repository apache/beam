package org.apache.beam.sdk.extensions.smb;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.CoderProvider;
import org.apache.beam.sdk.coders.CoderProviders;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

public class Test {
  public static void main(String[] args) throws IOException, CannotProvideCoderException {
    ObjectMapper objectMapper = new ObjectMapper();

    AvroBucketMetadata<String> metadata = new AvroBucketMetadata<>(
            10, String.class, BucketMetadata.HashType.MURMUR3_32, "a.b.c");
    String json = objectMapper.writeValueAsString(metadata);
    System.out.println("==========");
    System.out.println(json);

    metadata = objectMapper.readerFor(BucketMetadata.class).readValue(json);
    System.out.println("==========");
    System.out.println(metadata.getNumBuckets());
    System.out.println(metadata.getSortingKeyClass());
    System.out.println(metadata.getHashType());
    System.out.println(metadata.getSortingKeyCoder());
  }

}
