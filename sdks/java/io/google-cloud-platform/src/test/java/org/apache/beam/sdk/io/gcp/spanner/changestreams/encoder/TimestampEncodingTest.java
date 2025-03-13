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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.encoder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.cloud.Timestamp;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.AvroEncode;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.junit.Before;
import org.junit.Test;

public class TimestampEncodingTest {

  private TimestampEncoding encoding;

  @Before
  public void setUp() {
    encoding = new TimestampEncoding();
  }

  @Test
  public void testWriteAndReadTimestamp() throws IOException {
    final Timestamp expectedTimestamp = Timestamp.ofTimeSecondsAndNanos(10L, 20);
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    final BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(outputStream, null);

    encoding.write(expectedTimestamp, encoder);

    final byte[] bytes = outputStream.toByteArray();
    final ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
    final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
    final Timestamp actualTimestamp = encoding.read(null, decoder);

    assertEquals(expectedTimestamp, actualTimestamp);
  }

  @Test
  public void testWriteAndReadReuseTimestamp() throws IOException {
    final Timestamp expectedTimestamp = Timestamp.ofTimeSecondsAndNanos(10L, 20);
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    final BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(outputStream, null);

    encoding.write(expectedTimestamp, encoder);

    final byte[] bytes = outputStream.toByteArray();
    final ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
    final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
    final Timestamp actualTimestamp = encoding.read(Timestamp.now(), decoder);

    assertEquals(expectedTimestamp, actualTimestamp);
  }

  @Test
  public void testWriteAndReadNullTimestamp() throws IOException {
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    final BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(outputStream, null);

    encoding.write(null, encoder);

    final byte[] bytes = outputStream.toByteArray();
    final ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
    final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
    final Timestamp actualTimestamp = encoding.read(null, decoder);

    assertNull(actualTimestamp);
  }

  @Test(expected = ClassCastException.class)
  public void testThrowsExceptionWhenWritingNonTimestamp() throws IOException {
    encoding.write(1L, null);
  }

  @Test
  public void testReadAndWriteClassWithTimestampField() throws IOException {
    final AvroCoder<TestTimestamp> coder = AvroCoder.of(TestTimestamp.class);
    final List<TestTimestamp> allExpected =
        Arrays.asList(
            new TestTimestamp(Timestamp.MIN_VALUE, Timestamp.MAX_VALUE),
            new TestTimestamp(Timestamp.ofTimeSecondsAndNanos(123L, 456), Timestamp.now()),
            new TestTimestamp(Timestamp.MIN_VALUE, null),
            new TestTimestamp(null, Timestamp.MAX_VALUE));
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    for (TestTimestamp testTimestamp : allExpected) {
      coder.encode(testTimestamp, outputStream);
    }

    final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    for (TestTimestamp expected : allExpected) {
      final TestTimestamp actual = coder.decode(inputStream);
      assertEquals(expected, actual);
    }
  }

  private static class TestTimestamp {

    @AvroEncode(using = TimestampEncoding.class)
    private Timestamp timestamp1;

    @AvroEncode(using = TimestampEncoding.class)
    private Timestamp timestamp2;

    private TestTimestamp() {}

    private TestTimestamp(Timestamp timestamp1, Timestamp timestamp2) {
      this.timestamp1 = timestamp1;
      this.timestamp2 = timestamp2;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof TestTimestamp)) {
        return false;
      }
      TestTimestamp that = (TestTimestamp) o;
      return Objects.equals(timestamp1, that.timestamp1)
          && Objects.equals(timestamp2, that.timestamp2);
    }

    @Override
    public int hashCode() {
      return Objects.hash(timestamp1, timestamp2);
    }
  }
}
