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
package org.apache.beam.runners.flink.translation.types;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThrows;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.flink.api.common.typeutils.ComparatorTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.Test;

/** Tests {@link LengthPrefixedCoderTypeSerializer}. */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class LengthPrefixedCoderTypeSerializerTest implements Serializable {

  private static final AtomicInteger CODER_CALLS = new AtomicInteger();

  /** A String coder that counts how often it is invoked. */
  private static class CountingStringCoder extends AtomicCoder<String> {
    @Override
    public void encode(String value, OutputStream outStream) throws IOException {
      CODER_CALLS.incrementAndGet();
      StringUtf8Coder.of().encode(value, outStream);
    }

    @Override
    public String decode(InputStream inStream) throws IOException {
      CODER_CALLS.incrementAndGet();
      return StringUtf8Coder.of().decode(inStream);
    }
  }

  // Includes an empty record, a record that needs a multi-byte length and one that is larger than
  // the retained scratch buffer.
  private static final List<String> STRINGS =
      Arrays.asList("", "a", "b".repeat(300), "c".repeat(2 << 20), "d");

  @Test
  public void roundTripsRecordsWrittenBackToBack() throws IOException {
    LengthPrefixedCoderTypeSerializer<String> serializer =
        new LengthPrefixedCoderTypeSerializer<>(StringUtf8Coder.of(), false);
    DataOutputSerializer out = new DataOutputSerializer(64);
    for (String s : STRINGS) {
      serializer.serialize(s, out);
    }

    DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
    for (String s : STRINGS) {
      assertThat(serializer.deserialize(in), is(s));
    }
    assertThat(in.available(), is(0));
  }

  /**
   * Reads every remaining byte of its input, so it only sees one record if the stream ends there.
   */
  private static class RemainingBytesCoder extends AtomicCoder<byte[]> {
    @Override
    public void encode(byte[] value, OutputStream outStream) throws IOException {
      outStream.write(value);
    }

    @Override
    public byte[] decode(InputStream inStream) throws IOException {
      return inStream.readAllBytes();
    }
  }

  private static byte[] bytes(int length, int seed) {
    byte[] b = new byte[length];
    for (int i = 0; i < length; i++) {
      b[i] = (byte) (i * 31 + seed);
    }
    return b;
  }

  @Test
  public void reusedDecodeStreamSeesExactlyOneRecord() throws IOException {
    // Sizes cover empty records and a record above the 1 MiB retained-buffer limit, followed by
    // smaller records that reuse the stream.
    int[] sizes = {0, 1, 100, 3, (1 << 20) + 17, 5, 0, 2048};
    for (Coder<byte[]> coder :
        Arrays.<Coder<byte[]>>asList(ByteArrayCoder.of(), new RemainingBytesCoder())) {
      LengthPrefixedCoderTypeSerializer<byte[]> serializer =
          new LengthPrefixedCoderTypeSerializer<>(coder, false);
      DataOutputSerializer out = new DataOutputSerializer(64);
      for (int i = 0; i < sizes.length; i++) {
        serializer.serialize(bytes(sizes[i], i), out);
      }

      DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
      byte[][] decoded = new byte[sizes.length][];
      for (int i = 0; i < sizes.length; i++) {
        decoded[i] = serializer.deserialize(in);
      }
      assertThat(in.available(), is(0));
      // Checked after all records are decoded, so a value aliasing the reused buffer would fail.
      for (int i = 0; i < sizes.length; i++) {
        assertArrayEquals(bytes(sizes[i], i), decoded[i]);
      }
    }
  }

  @Test
  public void roundTripsNestedCoder() throws IOException {
    Coder<KV<String, Iterable<Long>>> coder =
        KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(VarLongCoder.of()));
    LengthPrefixedCoderTypeSerializer<KV<String, Iterable<Long>>> serializer =
        new LengthPrefixedCoderTypeSerializer<>(coder, false);
    KV<String, Iterable<Long>> first = KV.of("k1", Arrays.asList(1L, 2L, Long.MAX_VALUE));
    KV<String, Iterable<Long>> second = KV.of("k2", Arrays.asList());

    DataOutputSerializer out = new DataOutputSerializer(64);
    serializer.serialize(first, out);
    serializer.serialize(second, out);

    DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
    assertThat(serializer.deserialize(in), is(first));
    assertThat(serializer.deserialize(in), is(second));
  }

  @Test
  public void copiesBytesWithoutInvokingTheCoder() throws IOException {
    LengthPrefixedCoderTypeSerializer<String> serializer =
        new LengthPrefixedCoderTypeSerializer<>(new CountingStringCoder(), false);
    DataOutputSerializer original = new DataOutputSerializer(64);
    for (String s : STRINGS) {
      serializer.serialize(s, original);
    }
    byte[] originalBytes = original.getCopyOfBuffer();

    CODER_CALLS.set(0);
    DataInputDeserializer in = new DataInputDeserializer(originalBytes);
    // DataOutputSerializer#write(DataInputView, int) does not grow the buffer, so pre-size it.
    DataOutputSerializer copy = new DataOutputSerializer(originalBytes.length);
    for (int i = 0; i < STRINGS.size(); i++) {
      serializer.copy(in, copy);
    }

    assertThat(CODER_CALLS.get(), is(0));
    assertArrayEquals(originalBytes, copy.getCopyOfBuffer());
  }

  @Test
  public void throwsEofAtEndOfInput() {
    LengthPrefixedCoderTypeSerializer<String> serializer =
        new LengthPrefixedCoderTypeSerializer<>(StringUtf8Coder.of(), false);
    DataInputDeserializer in = new DataInputDeserializer(new byte[0]);
    assertThrows(EOFException.class, () -> serializer.deserialize(in));
  }

  @Test
  public void duplicateIsEqualButIndependent() throws IOException {
    LengthPrefixedCoderTypeSerializer<String> serializer =
        new LengthPrefixedCoderTypeSerializer<>(StringUtf8Coder.of(), false);
    LengthPrefixedCoderTypeSerializer<String> duplicate = serializer.duplicate();
    assertThat(duplicate, is(serializer));
    assertThat(duplicate, not(sameInstance(serializer)));

    // Interleaving the two instances must not corrupt either one's scratch buffers.
    DataOutputSerializer out1 = new DataOutputSerializer(64);
    DataOutputSerializer out2 = new DataOutputSerializer(64);
    serializer.serialize("first", out1);
    duplicate.serialize("second", out2);
    assertThat(
        serializer.deserialize(new DataInputDeserializer(out1.getCopyOfBuffer())), is("first"));
    assertThat(
        duplicate.deserialize(new DataInputDeserializer(out2.getCopyOfBuffer())), is("second"));
  }

  @Test
  public void isNotEqualToUnprefixedSerializer() {
    CoderTypeSerializer<String> plain = new CoderTypeSerializer<>(StringUtf8Coder.of(), false);
    LengthPrefixedCoderTypeSerializer<String> prefixed =
        new LengthPrefixedCoderTypeSerializer<>(StringUtf8Coder.of(), false);
    assertThat(plain, not(is((Object) prefixed)));
    assertThat(prefixed, not(is((Object) plain)));
  }

  @Test
  public void writesAndReadsSnapshot() throws IOException {
    LengthPrefixedCoderTypeSerializer<String> serializer =
        new LengthPrefixedCoderTypeSerializer<>(StringUtf8Coder.of(), false);

    TypeSerializerSnapshot writtenSnapshot = serializer.snapshotConfiguration();
    ComparatorTestBase.TestOutputView outView = new ComparatorTestBase.TestOutputView();
    writtenSnapshot.writeSnapshot(outView);

    TypeSerializerSnapshot readSnapshot = new UnversionedTypeSerializerSnapshot();
    readSnapshot.readSnapshot(
        writtenSnapshot.getCurrentVersion(), outView.getInputView(), getClass().getClassLoader());

    assertThat(readSnapshot.restoreSerializer(), is(serializer));
  }

  @Test
  public void typeInformationUsesLengthPrefixOnlyInBatchWhenEnabled() {
    assertThat(serializerFor(false, true), instanceOf(LengthPrefixedCoderTypeSerializer.class));
    assertThat(
        serializerFor(false, false), not(instanceOf(LengthPrefixedCoderTypeSerializer.class)));
    assertThat(serializerFor(true, true), not(instanceOf(LengthPrefixedCoderTypeSerializer.class)));
    assertThat(
        serializerFor(true, false), not(instanceOf(LengthPrefixedCoderTypeSerializer.class)));
  }

  private static CoderTypeSerializer<String> serializerFor(
      boolean streaming, boolean lengthPrefix) {
    FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
    options.setStreaming(streaming);
    options.setLengthPrefixBatchRecords(lengthPrefix);
    return CoderTypeSerializer.forTypeInformation(
        StringUtf8Coder.of(), new SerializablePipelineOptions(options), false);
  }
}
