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
package org.apache.beam.sdk.jmh.schemas;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Observer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.logicaltypes.Date;
import org.apache.beam.sdk.schemas.logicaltypes.DateTime;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.schemas.logicaltypes.MicrosInstant;
import org.apache.beam.sdk.schemas.logicaltypes.NanosDuration;
import org.apache.beam.sdk.schemas.logicaltypes.NanosInstant;
import org.apache.beam.sdk.schemas.logicaltypes.OneOfType;
import org.apache.beam.sdk.schemas.logicaltypes.Time;
import org.apache.beam.sdk.schemas.logicaltypes.Timestamp;
import org.apache.beam.sdk.schemas.logicaltypes.UuidLogicalType;
import org.apache.beam.sdk.util.common.ElementByteSizeObservableIterable;
import org.apache.beam.sdk.util.common.ElementByteSizeObservableIterator;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * JMH benchmarks for {@link RowCoder} and {@link SchemaCoder} encoding, decoding, and byte-size
 * observation across flat, nullable, static-encoding, nested, logical-type, POJO, and JavaBean
 * schemas.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@SuppressWarnings({"nullness", "rawtypes"})
public class SchemaCoderBenchmark {

  /** Reusable unsynchronized OutputStream reset per call to isolate coder CPU/allocations. */
  private static final class ResettableByteArrayOutputStream extends OutputStream {
    private byte[] buf;
    private int count;

    ResettableByteArrayOutputStream(int size) {
      this.buf = new byte[size];
    }

    @Override
    public void write(int b) {
      if (count >= buf.length) {
        buf = Arrays.copyOf(buf, buf.length << 1);
      }
      buf[count++] = (byte) b;
    }

    @Override
    public void write(byte[] b, int off, int len) {
      if (count + len > buf.length) {
        buf = Arrays.copyOf(buf, Math.max(buf.length << 1, count + len));
      }
      System.arraycopy(b, off, buf, count, len);
      count += len;
    }

    void reset() {
      count = 0;
    }

    int writtenSize() {
      return count;
    }
  }

  /** Reusable unsynchronized InputStream reset to position 0 to isolate decode costs. */
  private static final class ResettableByteArrayInputStream extends InputStream {
    private final byte[] buf;
    private int pos;

    ResettableByteArrayInputStream(byte[] buf) {
      this.buf = buf;
    }

    @Override
    public int read() {
      return pos < buf.length ? (buf[pos++] & 0xff) : -1;
    }

    @Override
    public int read(byte[] b, int off, int len) {
      if (pos >= buf.length) {
        return -1;
      }
      int avail = buf.length - pos;
      int n = Math.min(len, avail);
      System.arraycopy(buf, pos, b, off, n);
      pos += n;
      return n;
    }

    @Override
    public int available() {
      return buf.length - pos;
    }

    void rewind() {
      pos = 0;
    }
  }

  /** Counting {@link ElementByteSizeObserver} simulating Dataflow/runner size observer. */
  private static final class CountingByteSizeObserver extends ElementByteSizeObserver {
    long lastSize;

    @Override
    protected void reportElementSize(long elementSize) {
      lastSize += elementSize;
    }

    long resetAndAdvance() {
      advance();
      long r = lastSize;
      lastSize = 0;
      return r;
    }
  }

  /** Lazy observable iterable mimicking Dataflow GroupingShuffleReader / streaming state. */
  private static final class FakeLazyIterable
      extends ElementByteSizeObservableIterable<Row, ElementByteSizeObservableIterator<Row>> {
    private final List<Row> backing;

    FakeLazyIterable(List<Row> backing) {
      this.backing = backing;
    }

    @Override
    public void addObserver(Observer observer) {
      // Do not retain observers across millions of repeated benchmark calls; simply mirror the
      // superclass side effect of marking lazy byte-size observation without OOM accumulation.
    }

    @Override
    protected ElementByteSizeObservableIterator<Row> createIterator() {
      final Iterator<Row> delegate = backing.iterator();
      return new ElementByteSizeObservableIterator<Row>() {
        @Override
        public boolean hasNext() {
          return delegate.hasNext();
        }

        @Override
        public Row next() {
          Row r = delegate.next();
          notifyValueReturned(64L);
          return r;
        }
      };
    }
  }

  /** Enum for schema POJO/JavaBean benchmarks. */
  public enum Color {
    RED,
    GREEN,
    BLUE,
    YELLOW
  }

  /** Nested POJO for SchemaCoder benchmarks. */
  @DefaultSchema(JavaFieldSchema.class)
  public static class NestedPojo {
    public String city;
    public int zip;
    public double lat;

    public NestedPojo() {}

    public NestedPojo(String city, int zip, double lat) {
      this.city = city;
      this.zip = zip;
      this.lat = lat;
    }

    @Override
    public String toString() {
      return city + ":" + zip + ":" + lat;
    }
  }

  /** Representative flat primitive POJO matching typical Beam user records. */
  @DefaultSchema(JavaFieldSchema.class)
  public static class FlatPojo {
    public byte fByte;
    public short fShort;
    public int fInt;
    public long fLong;
    public float fFloat;
    public double fDouble;
    public boolean fBool;
    public String fString1;
    public String fString2;
    public Instant fInstant;

    public FlatPojo() {}
  }

  /** Rich POJO with containers, enum, nested POJO, UUID, and JSR-310 fields. */
  @DefaultSchema(JavaFieldSchema.class)
  public static class RichPojo {
    public long id;
    public String name;
    public double score;
    public boolean active;
    public Instant ts;
    public Color color;
    public UUID uuid;
    public LocalDateTime createdAt;
    public List<String> tags;
    public Map<String, Long> counts;
    public NestedPojo address;

    public RichPojo() {}
  }

  /** Classic mutable JavaBean (exercises SetterBasedCreatorFactory in JavaBeanSchema). */
  @DefaultSchema(JavaBeanSchema.class)
  public static class FlatJavaBean {
    private int fInt;
    private long fLong;
    private double fDouble;
    private boolean fBool;
    private String fString1;
    private String fString2;
    private Instant fInstant;
    private List<String> tags;
    private Map<String, Long> counts;

    public int getFInt() {
      return fInt;
    }

    public void setFInt(int v) {
      this.fInt = v;
    }

    public long getFLong() {
      return fLong;
    }

    public void setFLong(long v) {
      this.fLong = v;
    }

    public double getFDouble() {
      return fDouble;
    }

    public void setFDouble(double v) {
      this.fDouble = v;
    }

    public boolean isFBool() {
      return fBool;
    }

    public void setFBool(boolean v) {
      this.fBool = v;
    }

    public String getFString1() {
      return fString1;
    }

    public void setFString1(String v) {
      this.fString1 = v;
    }

    public String getFString2() {
      return fString2;
    }

    public void setFString2(String v) {
      this.fString2 = v;
    }

    public Instant getFInstant() {
      return fInstant;
    }

    public void setFInstant(Instant v) {
      this.fInstant = v;
    }

    public List<String> getTags() {
      return tags;
    }

    public void setTags(List<String> v) {
      this.tags = v;
    }

    public Map<String, Long> getCounts() {
      return counts;
    }

    public void setCounts(Map<String, Long> v) {
      this.counts = v;
    }
  }

  @State(Scope.Thread)
  public static class CoderState {
    Schema flatSchema;
    RowCoder flatCoder;
    Row flatRow;
    ResettableByteArrayInputStream flatIn;

    Schema staticSchema;
    RowCoder staticCoder;
    Row staticRow;
    ResettableByteArrayInputStream staticIn;

    Schema nullableSchema;
    RowCoder nullableCoder;
    Row nullableNoNullsRow;
    Row nullableSparseNullsRow;
    ResettableByteArrayInputStream nullableNoNullsIn;
    ResettableByteArrayInputStream nullableSparseNullsIn;

    Schema wideSchema;
    RowCoder wideCoder;
    Row wideRow;
    ResettableByteArrayInputStream wideIn;

    Schema nestedSchema;
    RowCoder nestedCoder;
    Row nestedRow;
    ResettableByteArrayInputStream nestedIn;

    Schema logicalSchema;
    RowCoder logicalCoder;
    Row logicalRow;
    ResettableByteArrayInputStream logicalIn;

    Schema groupedOutputSchema;
    RowCoder groupedOutputCoder;
    Row groupedOutputRow;

    SchemaCoder<FlatPojo> flatPojoCoder;
    FlatPojo flatPojo;
    ResettableByteArrayInputStream flatPojoIn;

    SchemaCoder<RichPojo> richPojoCoder;
    RichPojo richPojo;
    ResettableByteArrayInputStream richPojoIn;

    SchemaCoder<FlatJavaBean> javaBeanCoder;
    FlatJavaBean javaBean;
    ResettableByteArrayInputStream javaBeanIn;

    ResettableByteArrayOutputStream out;
    CountingByteSizeObserver observer;

    @Setup
    public void setUp() throws Exception {
      out = new ResettableByteArrayOutputStream(4096);
      observer = new CountingByteSizeObserver();

      flatSchema =
          Schema.builder()
              .addByteField("f_byte")
              .addInt16Field("f_short")
              .addInt32Field("f_int")
              .addInt64Field("f_long")
              .addFloatField("f_float")
              .addDoubleField("f_double")
              .addBooleanField("f_bool")
              .addStringField("f_str1")
              .addStringField("f_str2")
              .addDateTimeField("f_dt")
              .addByteArrayField("f_bytes")
              .addDecimalField("f_dec")
              .build();
      flatCoder = RowCoder.of(flatSchema);
      flatRow =
          Row.withSchema(flatSchema)
              .addValues(
                  (byte) 7,
                  (short) 42,
                  123456,
                  9_876_543_210L,
                  3.14f,
                  2.718281828,
                  true,
                  "us-central1",
                  "purchase-event-payload",
                  Instant.ofEpochMilli(1_700_000_000_000L),
                  "raw-bytes-123456".getBytes(StandardCharsets.UTF_8),
                  new BigDecimal("12345.67"))
              .build();
      flatIn = encodeToStream(flatCoder, flatRow);

      staticSchema =
          Schema.builder()
              .addInt32Field("f_int")
              .addInt64Field("f_long")
              .addDoubleField("f_double")
              .addBooleanField("f_bool")
              .addStringField("f_str")
              .addDateTimeField("f_dt")
              .setOptions(
                  Schema.Options.builder()
                      .setOption("beam:option:row:static_encoding", FieldType.BOOLEAN, true)
                      .build())
              .build();
      staticCoder = RowCoder.of(staticSchema);
      staticRow =
          Row.withSchema(staticSchema)
              .addValues(
                  42,
                  123_456_789L,
                  99.95,
                  false,
                  "order-item-123",
                  Instant.ofEpochMilli(1_700_000_000_000L))
              .build();
      staticIn = encodeToStream(staticCoder, staticRow);

      nullableSchema =
          Schema.builder()
              .addInt64Field("id")
              .addStringField("key")
              .addNullableInt32Field("n_int")
              .addNullableInt64Field("n_long")
              .addNullableDoubleField("n_double")
              .addNullableStringField("n_str1")
              .addNullableStringField("n_str2")
              .addNullableBooleanField("n_bool")
              .addNullableDateTimeField("n_dt")
              .addNullableByteArrayField("n_bytes")
              .build();
      nullableCoder = RowCoder.of(nullableSchema);
      nullableNoNullsRow =
          Row.withSchema(nullableSchema)
              .addValues(
                  1L,
                  "k1",
                  10,
                  200L,
                  1.5,
                  "alpha",
                  "beta",
                  true,
                  Instant.ofEpochMilli(1_700_000_000_000L),
                  new byte[] {1, 2, 3, 4})
              .build();
      nullableSparseNullsRow =
          Row.withSchema(nullableSchema)
              .addValues(
                  1L,
                  "k1",
                  10,
                  null,
                  1.5,
                  null,
                  "beta",
                  null,
                  Instant.ofEpochMilli(1_700_000_000_000L),
                  null)
              .build();
      nullableNoNullsIn = encodeToStream(nullableCoder, nullableNoNullsRow);
      nullableSparseNullsIn = encodeToStream(nullableCoder, nullableSparseNullsRow);

      Schema.Builder wideBuilder = Schema.builder();
      Object[] wideVals = new Object[40];
      for (int i = 0; i < 40; i++) {
        switch (i % 4) {
          case 0:
            wideBuilder.addInt64Field("l_" + i);
            wideVals[i] = 1000L + i;
            break;
          case 1:
            wideBuilder.addDoubleField("d_" + i);
            wideVals[i] = i * 1.25;
            break;
          case 2:
            wideBuilder.addInt32Field("i_" + i);
            wideVals[i] = i * 7;
            break;
          default:
            wideBuilder.addNullableStringField("s_" + i);
            wideVals[i] = (i % 8 == 3) ? null : "val_" + i;
            break;
        }
      }
      wideSchema = wideBuilder.build();
      wideCoder = RowCoder.of(wideSchema);
      wideRow = Row.withSchema(wideSchema).addValues(wideVals).build();
      wideIn = encodeToStream(wideCoder, wideRow);

      Schema childSchema =
          Schema.builder()
              .addStringField("name")
              .addInt32Field("qty")
              .addDoubleField("price")
              .build();
      nestedSchema =
          Schema.builder()
              .addInt64Field("orderId")
              .addRowField("primary", childSchema)
              .addArrayField("tags", FieldType.STRING)
              .addArrayField("items", FieldType.row(childSchema))
              .addMapField("attrs", FieldType.STRING, FieldType.INT64)
              .build();
      nestedCoder = RowCoder.of(nestedSchema);
      List<Row> items = new ArrayList<>();
      for (int i = 0; i < 5; i++) {
        items.add(Row.withSchema(childSchema).addValues("item-" + i, i + 1, 9.99 * i).build());
      }
      Map<String, Long> attrs = new HashMap<>();
      attrs.put("k1", 10L);
      attrs.put("k2", 20L);
      attrs.put("k3", 30L);
      attrs.put("k4", 40L);
      nestedRow =
          Row.withSchema(nestedSchema)
              .addValues(
                  42L,
                  Row.withSchema(childSchema).addValues("main", 3, 19.99).build(),
                  Arrays.asList("fast", "sale", "prime", "us"),
                  items,
                  attrs)
              .build();
      nestedIn = encodeToStream(nestedCoder, nestedRow);

      EnumerationType enumType = EnumerationType.create("RED", "GREEN", "BLUE", "YELLOW");
      OneOfType oneOfType =
          OneOfType.create(
              Schema.Field.of("str_val", FieldType.STRING),
              Schema.Field.of("int_val", FieldType.INT64),
              Schema.Field.of("dbl_val", FieldType.DOUBLE));
      logicalSchema =
          Schema.builder()
              .addLogicalTypeField("l_date", new Date())
              .addLogicalTypeField("l_time", new Time())
              .addLogicalTypeField("l_dt", new DateTime())
              .addLogicalTypeField("l_ts_micros", Timestamp.MICROS)
              .addLogicalTypeField("l_micros", new MicrosInstant())
              .addLogicalTypeField("l_nanos_inst", new NanosInstant())
              .addLogicalTypeField("l_nanos_dur", new NanosDuration())
              .addLogicalTypeField("l_uuid", new UuidLogicalType())
              .addLogicalTypeField("l_enum", enumType)
              .addLogicalTypeField("l_oneof", oneOfType)
              .build();
      logicalCoder = RowCoder.of(logicalSchema);
      logicalRow =
          Row.withSchema(logicalSchema)
              .addValues(
                  LocalDate.of(2026, 5, 15),
                  LocalTime.of(14, 30, 45, 123_000_000),
                  LocalDateTime.of(2026, 5, 15, 14, 30, 45),
                  java.time.Instant.ofEpochSecond(1_700_000_000L, 123_456_000L),
                  java.time.Instant.ofEpochSecond(1_700_000_000L, 654_321_000L),
                  java.time.Instant.ofEpochSecond(1_700_000_000L, 987_654_321L),
                  java.time.Duration.ofSeconds(120, 500_000_000L),
                  new UUID(0x123456789ABCDEF0L, 0x0FEDCBA987654321L),
                  enumType.valueOf("BLUE"),
                  oneOfType.createValue("int_val", 999L))
              .build();
      logicalIn = encodeToStream(logicalCoder, logicalRow);

      Schema keySchema = Schema.builder().addStringField("userId").build();
      groupedOutputSchema =
          Schema.builder()
              .addRowField("key", keySchema)
              .addIterableField("value", FieldType.row(flatSchema))
              .build();
      groupedOutputCoder = RowCoder.of(groupedOutputSchema);
      List<Row> groupedVals = new ArrayList<>(200);
      for (int i = 0; i < 200; i++) {
        groupedVals.add(flatRow);
      }
      groupedOutputRow =
          Row.withSchema(groupedOutputSchema)
              .attachValues(
                  Row.withSchema(keySchema).addValues("user-1").build(),
                  new FakeLazyIterable(groupedVals));

      SchemaRegistry registry = SchemaRegistry.createDefault();
      flatPojoCoder = registry.getSchemaCoder(FlatPojo.class);
      flatPojo = new FlatPojo();
      flatPojo.fByte = 7;
      flatPojo.fShort = 42;
      flatPojo.fInt = 123456;
      flatPojo.fLong = 9_876_543_210L;
      flatPojo.fFloat = 12.345f;
      flatPojo.fDouble = 67.8901234;
      flatPojo.fBool = true;
      flatPojo.fString1 = "us-central1";
      flatPojo.fString2 = "purchase-event-payload";
      flatPojo.fInstant = Instant.ofEpochMilli(1_700_000_000_000L);
      flatPojoIn = encodeToStream(flatPojoCoder, flatPojo);

      richPojoCoder = registry.getSchemaCoder(RichPojo.class);
      richPojo = new RichPojo();
      richPojo.id = 42L;
      richPojo.name = "widget";
      richPojo.score = 98.5;
      richPojo.active = true;
      richPojo.ts = Instant.ofEpochMilli(1_700_000_000_000L);
      richPojo.color = Color.BLUE;
      richPojo.uuid = new UUID(0x123456789ABCDEF0L, 0x0FEDCBA987654321L);
      richPojo.createdAt = LocalDateTime.of(2026, 5, 15, 14, 30, 0);
      richPojo.tags = Arrays.asList("a", "b", "c", "d");
      richPojo.counts = new HashMap<>(attrs);
      richPojo.address = new NestedPojo("Seattle", 98101, 47.6062);
      richPojoIn = encodeToStream(richPojoCoder, richPojo);

      javaBeanCoder = registry.getSchemaCoder(FlatJavaBean.class);
      javaBean = new FlatJavaBean();
      javaBean.setFInt(123456);
      javaBean.setFLong(9_876_543_210L);
      javaBean.setFDouble(67.8901234);
      javaBean.setFBool(true);
      javaBean.setFString1("us-central1");
      javaBean.setFString2("purchase-event-payload");
      javaBean.setFInstant(Instant.ofEpochMilli(1_700_000_000_000L));
      javaBean.setTags(Arrays.asList("a", "b", "c", "d"));
      javaBean.setCounts(new HashMap<>(attrs));
      javaBeanIn = encodeToStream(javaBeanCoder, javaBean);
    }

    private static <T> ResettableByteArrayInputStream encodeToStream(Coder<T> coder, T value)
        throws IOException {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(256);
      coder.encode(value, baos);
      return new ResettableByteArrayInputStream(baos.toByteArray());
    }
  }

  @Benchmark
  public int rowCoderFlatEncode(CoderState s) throws IOException {
    s.out.reset();
    s.flatCoder.encode(s.flatRow, s.out);
    return s.out.writtenSize();
  }

  @Benchmark
  public Row rowCoderFlatDecode(CoderState s) throws IOException {
    s.flatIn.rewind();
    return s.flatCoder.decode(s.flatIn);
  }

  @Benchmark
  public long rowCoderFlatByteSize(CoderState s) throws Exception {
    s.flatCoder.registerByteSizeObserver(s.flatRow, s.observer);
    return s.observer.resetAndAdvance();
  }

  @Benchmark
  public int rowCoderStaticEncode(CoderState s) throws IOException {
    s.out.reset();
    s.staticCoder.encode(s.staticRow, s.out);
    return s.out.writtenSize();
  }

  @Benchmark
  public Row rowCoderStaticDecode(CoderState s) throws IOException {
    s.staticIn.rewind();
    return s.staticCoder.decode(s.staticIn);
  }

  @Benchmark
  public int rowCoderNullableNoNullsEncode(CoderState s) throws IOException {
    s.out.reset();
    s.nullableCoder.encode(s.nullableNoNullsRow, s.out);
    return s.out.writtenSize();
  }

  @Benchmark
  public Row rowCoderNullableNoNullsDecode(CoderState s) throws IOException {
    s.nullableNoNullsIn.rewind();
    return s.nullableCoder.decode(s.nullableNoNullsIn);
  }

  @Benchmark
  public int rowCoderNullableSparseEncode(CoderState s) throws IOException {
    s.out.reset();
    s.nullableCoder.encode(s.nullableSparseNullsRow, s.out);
    return s.out.writtenSize();
  }

  @Benchmark
  public Row rowCoderNullableSparseDecode(CoderState s) throws IOException {
    s.nullableSparseNullsIn.rewind();
    return s.nullableCoder.decode(s.nullableSparseNullsIn);
  }

  @Benchmark
  public int rowCoderWide40Encode(CoderState s) throws IOException {
    s.out.reset();
    s.wideCoder.encode(s.wideRow, s.out);
    return s.out.writtenSize();
  }

  @Benchmark
  public Row rowCoderWide40Decode(CoderState s) throws IOException {
    s.wideIn.rewind();
    return s.wideCoder.decode(s.wideIn);
  }

  @Benchmark
  public int rowCoderNestedEncode(CoderState s) throws IOException {
    s.out.reset();
    s.nestedCoder.encode(s.nestedRow, s.out);
    return s.out.writtenSize();
  }

  @Benchmark
  public Row rowCoderNestedDecode(CoderState s) throws IOException {
    s.nestedIn.rewind();
    return s.nestedCoder.decode(s.nestedIn);
  }

  @Benchmark
  public int rowCoderLogicalTypesEncode(CoderState s) throws IOException {
    s.out.reset();
    s.logicalCoder.encode(s.logicalRow, s.out);
    return s.out.writtenSize();
  }

  @Benchmark
  public Row rowCoderLogicalTypesDecode(CoderState s) throws IOException {
    s.logicalIn.rewind();
    return s.logicalCoder.decode(s.logicalIn);
  }

  @Benchmark
  public long rowCoderLazyIterableByteSize(CoderState s) throws Exception {
    s.groupedOutputCoder.registerByteSizeObserver(s.groupedOutputRow, s.observer);
    boolean lazy = s.observer.getIsLazy();
    long size = s.observer.resetAndAdvance();
    return lazy ? 1L : size;
  }

  @Benchmark
  public int schemaCoderFlatPojoEncode(CoderState s) throws IOException {
    s.out.reset();
    s.flatPojoCoder.encode(s.flatPojo, s.out);
    return s.out.writtenSize();
  }

  @Benchmark
  public FlatPojo schemaCoderFlatPojoDecode(CoderState s) throws IOException {
    s.flatPojoIn.rewind();
    return s.flatPojoCoder.decode(s.flatPojoIn);
  }

  @Benchmark
  public int schemaCoderRichPojoEncode(CoderState s) throws IOException {
    s.out.reset();
    s.richPojoCoder.encode(s.richPojo, s.out);
    return s.out.writtenSize();
  }

  @Benchmark
  public RichPojo schemaCoderRichPojoDecode(CoderState s) throws IOException {
    s.richPojoIn.rewind();
    return s.richPojoCoder.decode(s.richPojoIn);
  }

  @Benchmark
  public int schemaCoderJavaBeanEncode(CoderState s) throws IOException {
    s.out.reset();
    s.javaBeanCoder.encode(s.javaBean, s.out);
    return s.out.writtenSize();
  }

  @Benchmark
  public FlatJavaBean schemaCoderJavaBeanDecode(CoderState s) throws IOException {
    s.javaBeanIn.rewind();
    return s.javaBeanCoder.decode(s.javaBeanIn);
  }
}
