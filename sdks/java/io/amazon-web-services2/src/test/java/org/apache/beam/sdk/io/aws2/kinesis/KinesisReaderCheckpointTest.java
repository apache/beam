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
package org.apache.beam.sdk.io.aws2.kinesis;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.junit.Test;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

public class KinesisReaderCheckpointTest {
  /**
   * This was generated with Beam master branch.
   * https://github.com/apache/beam/commit/ca0787642a6b3804a742326147281c99ae8d08d2
   */
  private static final String OLDER_VERSION_SERIALIZED_CHECKPOINT =
      "rO0ABXNyADtvcmcuYXBhY2hlLmJlYW0uc2RrLmlvLmF3czIua2luZXNpcy5LaW5lc2lzUmVhZGVyQ2hlY2twb2ludKHLb3bO/6XJAgABTAAQc2hhcmRDaGVja3BvaW50c3QAEExqYXZhL3V0aWwvTGlzdDt4cHNyAF1vcmcuYXBhY2hlLmJlYW0udmVuZG9yLmd1YXZhLnYyNl8wX2pyZS5jb20uZ29vZ2xlLmNvbW1vbi5jb2xsZWN0LkltbXV0YWJsZUxpc3QkU2VyaWFsaXplZEZvcm0AAAAAAAAAAAIAAVsACGVsZW1lbnRzdAATW0xqYXZhL2xhbmcvT2JqZWN0O3hwdXIAE1tMamF2YS5sYW5nLk9iamVjdDuQzlifEHMpbAIAAHhwAAAAAXNyADNvcmcuYXBhY2hlLmJlYW0uc2RrLmlvLmF3czIua2luZXNpcy5TaGFyZENoZWNrcG9pbnQBb9XvUdq1BwIABkwADnNlcXVlbmNlTnVtYmVydAASTGphdmEvbGFuZy9TdHJpbmc7TAAHc2hhcmRJZHEAfgAJTAARc2hhcmRJdGVyYXRvclR5cGV0AEFMc29mdHdhcmUvYW1hem9uL2F3c3Nkay9zZXJ2aWNlcy9raW5lc2lzL21vZGVsL1NoYXJkSXRlcmF0b3JUeXBlO0wACnN0cmVhbU5hbWVxAH4ACUwAEXN1YlNlcXVlbmNlTnVtYmVydAAQTGphdmEvbGFuZy9Mb25nO0wACXRpbWVzdGFtcHQAF0xvcmcvam9kYS90aW1lL0luc3RhbnQ7eHB0AAI0MnQACXNoYXJkLTAwMH5yAD9zb2Z0d2FyZS5hbWF6b24uYXdzc2RrLnNlcnZpY2VzLmtpbmVzaXMubW9kZWwuU2hhcmRJdGVyYXRvclR5cGUAAAAAAAAAABIAAHhyAA5qYXZhLmxhbmcuRW51bQAAAAAAAAAAEgAAeHB0ABVBRlRFUl9TRVFVRU5DRV9OVU1CRVJ0AAlzdHJlYW0tMDFzcgAOamF2YS5sYW5nLkxvbmc7i+SQzI8j3wIAAUoABXZhbHVleHIAEGphdmEubGFuZy5OdW1iZXKGrJUdC5TgiwIAAHhwAAAAAAAAAAxw";

  private ShardCheckpoint a =
      new ShardCheckpoint(
          "stream-01", "shard-000", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "40", 12L);
  private ShardCheckpoint b =
      new ShardCheckpoint(
          "stream-01", "shard-001", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "41", 12L);
  private ShardCheckpoint c =
      new ShardCheckpoint(
          "stream-01", "shard-002", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "42", 12L);

  @Test
  public void splitsCheckpointAccordingly() {
    KinesisReaderCheckpoint checkpoint = new KinesisReaderCheckpoint(asList(a, b, c));
    verifySplitInto(checkpoint, 1);
    verifySplitInto(checkpoint, 2);
    verifySplitInto(checkpoint, 3);
    verifySplitInto(checkpoint, 4);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void isImmutable() {
    KinesisReaderCheckpoint checkpoint = new KinesisReaderCheckpoint(asList(a, b, c));
    Iterator<ShardCheckpoint> iterator = checkpoint.iterator();
    iterator.remove();
  }

  @Test
  public void testJavaSerialization() throws IOException, ClassNotFoundException {
    ShardCheckpoint shardCheckpoint =
        new ShardCheckpoint(
            "stream-01", "shard-000", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "42", 12L);
    KinesisReaderCheckpoint checkpoint =
        new KinesisReaderCheckpoint(ImmutableList.of(shardCheckpoint));

    String serializedCheckpoint = serializeObjectToString(checkpoint);
    KinesisReaderCheckpoint deserializedCheckpoint =
        (KinesisReaderCheckpoint) deSerializeObjectFromString(serializedCheckpoint);

    KinesisReaderCheckpoint olderVersionDeserializedCheckpoint =
        (KinesisReaderCheckpoint) deSerializeObjectFromString(OLDER_VERSION_SERIALIZED_CHECKPOINT);

    assertThat(checkpoint).containsExactlyInAnyOrder(shardCheckpoint);
    assertThat(deserializedCheckpoint).containsExactlyInAnyOrder(shardCheckpoint);
    assertThat(olderVersionDeserializedCheckpoint).containsExactlyInAnyOrder(shardCheckpoint);
  }

  private void verifySplitInto(KinesisReaderCheckpoint checkpoint, int size) {
    List<KinesisReaderCheckpoint> split = checkpoint.splitInto(size);
    assertThat(Iterables.concat(split)).containsOnly(a, b, c);
    assertThat(split).hasSize(Math.min(size, 3));
  }

  private static String serializeObjectToString(Serializable o) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(outputStream);
    oos.writeObject(o);
    oos.close();
    return Base64.getEncoder().encodeToString(outputStream.toByteArray());
  }

  private static Object deSerializeObjectFromString(String s)
      throws IOException, ClassNotFoundException {
    byte[] data = Base64.getDecoder().decode(s);
    ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));
    Object o = ois.readObject();
    ois.close();
    return o;
  }
}
