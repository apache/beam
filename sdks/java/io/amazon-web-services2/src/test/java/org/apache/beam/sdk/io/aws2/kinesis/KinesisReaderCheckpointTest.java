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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.junit.Test;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

public class KinesisReaderCheckpointTest {

  /** This was generated manually. */
  private static final String OLDER_VERSION_SERIALIZED_CHECKPOINT =
      "rO0ABXNyADtvcmcuYXBhY2hlLmJlYW0uc2RrLmlvLmF3czIua2luZXNpcy5LaW5lc2lzUmVhZGVyQ2hlY2twb2ludKHLb3bO/6XJAgABTAAQc2hhcmRDaGVja3BvaW50c3QAEExqYXZhL3V0aWwvTGlzdDt4cHNyAF9vcmcuYXBhY2hlLmJlYW0udmVuZG9yLmd1YXZhLnYzMl8xXzJfanJlLmNvbS5nb29nbGUuY29tbW9uLmNvbGxlY3QuSW1tdXRhYmxlTGlzdCRTZXJpYWxpemVkRm9ybQAAAAAAAAAAAgABWwAIZWxlbWVudHN0ABNbTGphdmEvbGFuZy9PYmplY3Q7eHB1cgATW0xqYXZhLmxhbmcuT2JqZWN0O5DOWJ8QcylsAgAAeHAAAAABc3IAM29yZy5hcGFjaGUuYmVhbS5zZGsuaW8uYXdzMi5raW5lc2lzLlNoYXJkQ2hlY2twb2ludAFv1e9R2rUHAgAGTAAOc2VxdWVuY2VOdW1iZXJ0ABJMamF2YS9sYW5nL1N0cmluZztMAAdzaGFyZElkcQB+AAlMABFzaGFyZEl0ZXJhdG9yVHlwZXQAQUxzb2Z0d2FyZS9hbWF6b24vYXdzc2RrL3NlcnZpY2VzL2tpbmVzaXMvbW9kZWwvU2hhcmRJdGVyYXRvclR5cGU7TAAKc3RyZWFtTmFtZXEAfgAJTAARc3ViU2VxdWVuY2VOdW1iZXJ0ABBMamF2YS9sYW5nL0xvbmc7TAAJdGltZXN0YW1wdAAXTG9yZy9qb2RhL3RpbWUvSW5zdGFudDt4cHQAAjQydAAJc2hhcmQtMDAwfnIAP3NvZnR3YXJlLmFtYXpvbi5hd3NzZGsuc2VydmljZXMua2luZXNpcy5tb2RlbC5TaGFyZEl0ZXJhdG9yVHlwZQAAAAAAAAAAEgAAeHIADmphdmEubGFuZy5FbnVtAAAAAAAAAAASAAB4cHQAFUFGVEVSX1NFUVVFTkNFX05VTUJFUnQACXN0cmVhbS0wMXNyAA5qYXZhLmxhbmcuTG9uZzuL5JDMjyPfAgABSgAFdmFsdWV4cgAQamF2YS5sYW5nLk51bWJlcoaslR0LlOCLAgAAeHAAAAAAAAAADHA=";

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
    assertThat(checkpoint).containsExactlyInAnyOrder(shardCheckpoint);
    assertThat(deserializedCheckpoint).containsExactlyInAnyOrder(shardCheckpoint);

    try {
      KinesisReaderCheckpoint olderVersionDeserializedCheckpoint =
          (KinesisReaderCheckpoint)
              deSerializeObjectFromString(OLDER_VERSION_SERIALIZED_CHECKPOINT);
      assertThat(olderVersionDeserializedCheckpoint).containsExactlyInAnyOrder(shardCheckpoint);
    } catch (ClassNotFoundException e) {
      String errorMessage =
          String.format(
              "KinesisReaderCheckpoint may have changed. Consider update OLDER_VERSION_SERIALIZED_CHECKPOINT: \"%s\"",
              serializedCheckpoint);
      throw new RuntimeException(errorMessage, e);
    }
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
