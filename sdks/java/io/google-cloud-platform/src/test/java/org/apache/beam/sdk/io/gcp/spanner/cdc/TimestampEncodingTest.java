package org.apache.beam.sdk.io.gcp.spanner.cdc;

import static org.junit.Assert.assertEquals;

import com.google.cloud.Timestamp;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Objects;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.AvroEncode;
import org.apache.beam.sdk.coders.AvroCoder;
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

  @Test(expected = ClassCastException.class)
  public void testThrowsExceptionWhenWritingNonTimestamp() throws IOException {
    encoding.write(1L, null);
  }

  @Test
  public void testReadAndWriteClassWithTimestampField() throws IOException {
    final AvroCoder<TestTimestamp> coder = AvroCoder.of(TestTimestamp.class);
    final TestTimestamp expected = new TestTimestamp(Timestamp.ofTimeSecondsAndNanos(123L, 456));
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    coder.encode(expected, outputStream);

    final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    final TestTimestamp actual = coder.decode(inputStream);

    assertEquals(expected, actual);
  }

  private static class TestTimestamp {

    @AvroEncode(using = TimestampEncoding.class)
    private Timestamp timestamp;

    private TestTimestamp() {
    }

    private TestTimestamp(Timestamp timestamp) {
      this.timestamp = timestamp;
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
      return Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
      return Objects.hash(timestamp);
    }
  }
}
