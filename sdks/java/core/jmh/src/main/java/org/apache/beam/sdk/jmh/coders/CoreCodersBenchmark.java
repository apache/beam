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

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.util.ExposedByteArrayInputStream;
import org.apache.beam.sdk.util.ExposedByteArrayOutputStream;
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
 * Microbenchmarks for {@link LengthPrefixCoder}, {@link BigEndianLongCoder}, {@link InstantCoder},
 * and {@link WindowedValues} coders.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class CoreCodersBenchmark {

  /** Reusable unsynchronized byte[] sink that copies written bytes without reallocating. */
  private static final class ResettableByteSink extends OutputStream {
    final byte[] buf = new byte[1024];
    int pos;

    void reset() {
      pos = 0;
    }

    @Override
    public void write(int b) {
      buf[pos++] = (byte) b;
    }

    @Override
    public void write(byte[] b, int off, int len) {
      System.arraycopy(b, off, buf, pos, len);
      pos += len;
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

  /** Pass-through FilterInputStream to force megamorphic InputStream dispatch. */
  private static final class PassthroughInputStream extends FilterInputStream {
    PassthroughInputStream(InputStream in) {
      super(in);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return in.read(b, off, len);
    }
  }

  /** Pass-through FilterOutputStream to force megamorphic OutputStream dispatch. */
  private static final class PassthroughOutputStream extends FilterOutputStream {
    PassthroughOutputStream(OutputStream out) {
      super(out);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      out.write(b, off, len);
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

    final ResettableByteSink sink = new ResettableByteSink();
    final ByteArrayOutputStream syncBaos = new ByteArrayOutputStream(1024);
    final ExposedByteArrayOutputStream exposedBaos = new ExposedByteArrayOutputStream();
    final ByteStringOutputStream byteStringOut = new ByteStringOutputStream(1024);
    final ResettableByteSink passthroughBackingSink = new ResettableByteSink();
    final PassthroughOutputStream passthroughOut =
        new PassthroughOutputStream(passthroughBackingSink);
    OutputStream[] polyOuts;
    int polyOutIdx;

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

    byte[] encodedLongBytes;
    InputStream[] polyLongIns;
    ResettableByteArrayInputStream[] polyLongBackings;
    int polyInIdx;

    @Setup
    public void setUp() throws IOException {
      byteArrayValue = new byte[256];
      for (int i = 0; i < byteArrayValue.length; i++) {
        byteArrayValue[i] = (byte) (i * 31 + 17);
      }
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

      ByteArrayOutputStream longBaos = new ByteArrayOutputStream();
      longCoder.encode(longValue, longBaos);
      encodedLongBytes = longBaos.toByteArray();

      // Four distinct concrete InputStream types so BitConverters.readBigEndianLong is megamorphic.
      ResettableByteArrayInputStream backing0 =
          new ResettableByteArrayInputStream(encodedLongBytes);
      ResettableByteArrayInputStream backing1 =
          new ResettableByteArrayInputStream(encodedLongBytes);
      ExposedByteArrayInputStream exposedIn = new ExposedByteArrayInputStream(encodedLongBytes);
      ResettableByteArrayInputStream backing3 =
          new ResettableByteArrayInputStream(encodedLongBytes);
      polyLongBackings = new ResettableByteArrayInputStream[] {backing0, backing1, null, backing3};
      polyLongIns =
          new InputStream[] {
            backing0,
            new PassthroughInputStream(backing1),
            exposedIn,
            new BufferedInputStream(backing3, 64) {
              @Override
              public synchronized void reset() {
                pos = 0;
                count = 0;
              }
            }
          };

      // Four distinct concrete OutputStream types (including synchronized BAOS/ExposedBAOS and
      // ByteStringOutputStream) so BitConverters.writeBigEndianLong is megamorphic.
      polyOuts = new OutputStream[] {syncBaos, exposedBaos, byteStringOut, passthroughOut};
    }

    InputStream nextPolyLongInput() throws IOException {
      int idx = polyInIdx++ & 3;
      if (idx == 0 || idx == 1) {
        polyLongBackings[idx].resetToStart();
      } else if (idx == 2) {
        ((ExposedByteArrayInputStream) polyLongIns[2]).reset();
      } else {
        polyLongBackings[3].resetToStart();
        polyLongIns[3].reset();
      }
      return polyLongIns[idx];
    }

    OutputStream nextPolyOutput() {
      int idx = polyOutIdx++ & 3;
      if (idx == 0) {
        syncBaos.reset();
      } else if (idx == 1) {
        exposedBaos.reset();
      } else if (idx == 2) {
        byteStringOut.reset();
      } else {
        passthroughBackingSink.reset();
      }
      return polyOuts[idx];
    }

    private static <T> ResettableByteArrayInputStream encodeToStream(Coder<T> coder, T value)
        throws IOException {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      coder.encode(value, baos);
      return new ResettableByteArrayInputStream(baos.toByteArray());
    }
  }

  @Benchmark
  public int lengthPrefixByteArrayEncode(CoderState s) throws IOException {
    s.sink.reset();
    s.lpByteArrayCoder.encode(s.byteArrayValue, s.sink);
    return s.sink.pos;
  }

  @Benchmark
  public byte[] lengthPrefixByteArrayDecode(CoderState s) throws IOException {
    s.encodedLpByteArray.resetToStart();
    return s.lpByteArrayCoder.decode(s.encodedLpByteArray);
  }

  @Benchmark
  public int lengthPrefixStringEncode(CoderState s) throws IOException {
    s.sink.reset();
    s.lpStringCoder.encode(s.stringValue, s.sink);
    return s.sink.pos;
  }

  @Benchmark
  public String lengthPrefixStringDecode(CoderState s) throws IOException {
    s.encodedLpString.resetToStart();
    return s.lpStringCoder.decode(s.encodedLpString);
  }

  @Benchmark
  public int lengthPrefixKvEncode(CoderState s) throws IOException {
    s.sink.reset();
    s.lpKvCoder.encode(s.kvValue, s.sink);
    return s.sink.pos;
  }

  @Benchmark
  public KV<String, Long> lengthPrefixKvDecode(CoderState s) throws IOException {
    s.encodedLpKv.resetToStart();
    return s.lpKvCoder.decode(s.encodedLpKv);
  }

  @Benchmark
  public int lengthPrefixNestedEncode(CoderState s) throws IOException {
    s.sink.reset();
    s.lpNestedCoder.encode(s.nestedKvValue, s.sink);
    return s.sink.pos;
  }

  @Benchmark
  public int bigEndianLongEncode(CoderState s) throws IOException {
    s.byteStringOut.reset();
    s.longCoder.encode(s.longValue, s.byteStringOut);
    return s.byteStringOut.size();
  }

  @Benchmark
  public void bigEndianLongEncodePolymorphic(CoderState s, Blackhole bh) throws IOException {
    OutputStream out = s.nextPolyOutput();
    s.longCoder.encode(s.longValue, out);
    bh.consume(out);
  }

  @Benchmark
  public Long bigEndianLongDecode(CoderState s) throws IOException {
    s.encodedLong.resetToStart();
    return s.longCoder.decode(s.encodedLong);
  }

  @Benchmark
  public Long bigEndianLongDecodePolymorphic(CoderState s) throws IOException {
    return s.longCoder.decode(s.nextPolyLongInput());
  }

  @Benchmark
  public int instantEncode(CoderState s) throws IOException {
    s.byteStringOut.reset();
    s.instantCoder.encode(s.instantValue, s.byteStringOut);
    return s.byteStringOut.size();
  }

  @Benchmark
  public Instant instantDecode(CoderState s) throws IOException {
    s.encodedInstant.resetToStart();
    return s.instantCoder.decode(s.encodedInstant);
  }

  @Benchmark
  public int windowedValueEncode(CoderState s) throws IOException {
    s.sink.reset();
    s.windowedValueCoder.encode(s.windowedValue, s.sink);
    return s.sink.pos;
  }

  @Benchmark
  public void windowedValueDecode(CoderState s, Blackhole bh) throws IOException {
    s.encodedWindowedValue.resetToStart();
    bh.consume(s.windowedValueCoder.decode(s.encodedWindowedValue));
  }
}
