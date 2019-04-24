package org.apache.beam.sdk.extensions.smb;

import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;

import java.util.Iterator;

// @Todo test iterator
public class TestBucketSourceReader {
  public static void main(String[] args) throws Exception {
    /*TestSortedBucketFile.Reader reader = new TestSortedBucketFile.Reader(Lists.newArrayList(
        "a1", "a1", "a2", "a3",
        "b1", "b2", "b2", "b3",
        "c1",
        "d1",
        "e1", "e2"
    ));
    SortedBucketSource.BucketSourceReader<String, String> bucketSourceReader =
        new SortedBucketSource.BucketSourceReader<>(
            reader, s -> s.substring(0, 1));

    KV<String, Iterator<String>> kv = bucketSourceReader.nextKeyGroup();
    while (kv != null) {
      System.out.println("KEY: " + kv.getKey());
      kv.getValue().forEachRemaining(System.out::println);
      kv = bucketSourceReader.nextKeyGroup();
    }*/
  }
}
