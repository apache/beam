package org.apache.beam.sdk.extensions.ordered.combiner;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.beam.sdk.extensions.ordered.combiner.SequenceRangeAccumulator.SequenceRangeAccumulatorCoder;
import org.joda.time.Instant;
import org.junit.Test;

public class SequenceRangeAccumulatorCoderTest {

  private SequenceRangeAccumulatorCoder coder = SequenceRangeAccumulatorCoder.of();

  @Test
  public void testEncodingEmptyAccumulator() throws IOException {
    SequenceRangeAccumulator empty = new SequenceRangeAccumulator();

    doTestEncodingAndDecoding(empty);
  }

  @Test
  public void testEncodingAccumulatorWithoutInitialSequence() throws IOException {
    SequenceRangeAccumulator accumulator = new SequenceRangeAccumulator();
    accumulator.add(1, Instant.now(), false);
    accumulator.add(2, Instant.now(), false);
    accumulator.add(3, Instant.now(), false);
    accumulator.add(5, Instant.now(), false);
    accumulator.add(6, Instant.now(), false);

    doTestEncodingAndDecoding(accumulator);
  }

  @Test
  public void testEncodingAccumulatorWithInitialSequence() throws IOException {
    SequenceRangeAccumulator accumulator = new SequenceRangeAccumulator();
    accumulator.add(1, Instant.now(), true);
    accumulator.add(2, Instant.now(), false);
    accumulator.add(3, Instant.now(), false);
    accumulator.add(5, Instant.now(), false);
    accumulator.add(6, Instant.now(), false);

    doTestEncodingAndDecoding(accumulator);
  }

  private void doTestEncodingAndDecoding(SequenceRangeAccumulator value)
      throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    coder.encode(value, output);

    SequenceRangeAccumulator decoded = coder.decode(new ByteArrayInputStream(output.toByteArray()));
    assertEquals("Accumulator", value, decoded);
  }

}
