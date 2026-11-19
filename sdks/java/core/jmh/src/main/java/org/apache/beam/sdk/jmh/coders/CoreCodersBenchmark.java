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
package org.apache.beam.sdk.jmh.coders;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Microbenchmarks for {@link LengthPrefixCoder}, {@link BigEndianLongCoder}, and {@link
 * WindowedValues} coders.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class CoreCodersBenchmark {

  /** Reusable blackhole OutputStream that performs zero allocations. */
  private static final class CountingOutputStream extends OutputStream {
    long count;

    @Override
    public void write(int b) {
      count++;
    }

    @Override
    public void write(byte[] b, int off, int len) {
      count += len;
    }
  }

  /** Reusable ByteArrayInputStream to avoid benchmark harness allocations during decode. */
  private static final class ResettableByteArrayInputStream extends ByteArrayInputStream {
    ResettableByteArrayInputStream(byte[] buf) {
      super(buf);
    }

    void resetToStart() {
      this.pos = 0;
    }
  }

  @State(Scope.Thread)
  public static class CoderState {
    final LengthPrefixCoder<byte[]> lpByteArrayCoder = LengthPrefixCoder.of(ByteArrayCoder.of());
    final LengthPrefixCoder<String> lpStringCoder = LengthPrefixCoder.of(StringUtf8Coder.of());
    final LengthPrefixCoder<KV<String, Long>> lpKvCoder =
        LengthPrefixCoder.of(KvCoder.of(StringUtf8Coder.of(), BigEndianLongCoder.of()));
    final LengthPrefixCoder<KV<String, byte[]>> lpNestedCoder =
        LengthPrefixCoder.of(
            KvCoder.of(
                LengthPrefixCoder.of(StringUtf8Coder.of()),
                LengthPrefixCoder.of(ByteArrayCoder.of())));

    final BigEndianLongCoder longCoder = BigEndianLongCoder.of();
    final InstantCoder instantCoder = InstantCoder.of();
    final Coder<WindowedValue<String>> windowedValueCoder =
        WindowedValues.getFullCoder(StringUtf8Coder.of(), IntervalWindow.getCoder());

    final CountingOutputStream out = new CountingOutputStream();

    byte[] byteArrayValue;
    String stringValue;
    KV<String, Long> kvValue;
    KV<String, byte[]> nestedKvValue;
    Long longValue;
    Instant instantValue;
    WindowedValue<String> windowedValue;

    ResettableByteArrayInputStream encodedLpByteArray;
    ResettableByteArrayInputStream encodedLpString;
    ResettableByteArrayInputStream encodedLpKv;
    ResettableByteArrayInputStream encodedLong;
    ResettableByteArrayInputStream encodedInstant;
    ResettableByteArrayInputStream encodedWindowedValue;

    @Setup
    public void setUp() throws IOException {
      Random rng = new Random(42);
      byteArrayValue = new byte[256];
      rng.nextBytes(byteArrayValue);
      stringValue = "apache-beam-performance-benchmark-key-0123456789-abcdef-utf8-\u00e9\u00fc";
      kvValue = KV.of(stringValue, 123456789012345L);
      nestedKvValue = KV.of(stringValue, byteArrayValue);
      longValue = 987654321098765432L;
      instantValue = new Instant(1_700_000_000_000L);
      windowedValue =
          WindowedValues.of(
              stringValue,
              instantValue,
              new IntervalWindow(instantValue, instantValue.plus(Duration.standardMinutes(1))),
              PaneInfo.NO_FIRING);

      encodedLpByteArray = encodeToStream(lpByteArrayCoder, byteArrayValue);
      encodedLpString = encodeToStream(lpStringCoder, stringValue);
      encodedLpKv = encodeToStream(lpKvCoder, kvValue);
      encodedLong = encodeToStream(longCoder, longValue);
      encodedInstant = encodeToStream(instantCoder, instantValue);
      encodedWindowedValue = encodeToStream(windowedValueCoder, windowedValue);
    }

    private static <T> ResettableByteArrayInputStream encodeToStream(Coder<T> coder, T value)
        throws IOException {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      coder.encode(value, baos);
      return new ResettableByteArrayInputStream(baos.toByteArray());
    }
  }

  @Benchmark
  public long lengthPrefixByteArrayEncode(CoderState s) throws IOException {
    s.out.count = 0;
    s.lpByteArrayCoder.encode(s.byteArrayValue, s.out);
    return s.out.count;
  }

  @Benchmark
  public byte[] lengthPrefixByteArrayDecode(CoderState s) throws IOException {
    s.encodedLpByteArray.resetToStart();
    return s.lpByteArrayCoder.decode(s.encodedLpByteArray);
  }

  @Benchmark
  public long lengthPrefixStringEncode(CoderState s) throws IOException {
    s.out.count = 0;
    s.lpStringCoder.encode(s.stringValue, s.out);
    return s.out.count;
  }

  @Benchmark
  public String lengthPrefixStringDecode(CoderState s) throws IOException {
    s.encodedLpString.resetToStart();
    return s.lpStringCoder.decode(s.encodedLpString);
  }

  @Benchmark
  public long lengthPrefixKvEncode(CoderState s) throws IOException {
    s.out.count = 0;
    s.lpKvCoder.encode(s.kvValue, s.out);
    return s.out.count;
  }

  @Benchmark
  public KV<String, Long> lengthPrefixKvDecode(CoderState s) throws IOException {
    s.encodedLpKv.resetToStart();
    return s.lpKvCoder.decode(s.encodedLpKv);
  }

  @Benchmark
  public long lengthPrefixNestedEncode(CoderState s) throws IOException {
    s.out.count = 0;
    s.lpNestedCoder.encode(s.nestedKvValue, s.out);
    return s.out.count;
  }

  @Benchmark
  public long bigEndianLongEncode(CoderState s) throws IOException {
    s.out.count = 0;
    s.longCoder.encode(s.longValue, s.out);
    return s.out.count;
  }

  @Benchmark
  public Long bigEndianLongDecode(CoderState s) throws IOException {
    s.encodedLong.resetToStart();
    return s.longCoder.decode(s.encodedLong);
  }

  @Benchmark
  public long instantEncode(CoderState s) throws IOException {
    s.out.count = 0;
    s.instantCoder.encode(s.instantValue, s.out);
    return s.out.count;
  }

  @Benchmark
  public Instant instantDecode(CoderState s) throws IOException {
    s.encodedInstant.resetToStart();
    return s.instantCoder.decode(s.encodedInstant);
  }

  @Benchmark
  public long windowedValueEncode(CoderState s) throws IOException {
    s.out.count = 0;
    s.windowedValueCoder.encode(s.windowedValue, s.out);
    return s.out.count;
  }

  @Benchmark
  public void windowedValueDecode(CoderState s, Blackhole bh) throws IOException {
    s.encodedWindowedValue.resetToStart();
    bh.consume(s.windowedValueCoder.decode(s.encodedWindowedValue));
  }
}
