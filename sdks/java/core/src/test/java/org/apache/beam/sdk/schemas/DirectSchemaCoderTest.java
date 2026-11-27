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
package org.apache.beam.sdk.schemas;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.BigEndianShortCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.RowCoderGenerator;
import org.apache.beam.sdk.coders.RowWireCodec;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.logicaltypes.Date;
import org.apache.beam.sdk.schemas.logicaltypes.MicrosInstant;
import org.apache.beam.sdk.schemas.logicaltypes.NanosDuration;
import org.apache.beam.sdk.schemas.logicaltypes.NanosInstant;
import org.apache.beam.sdk.schemas.logicaltypes.OneOfType;
import org.apache.beam.sdk.schemas.logicaltypes.Time;
import org.apache.beam.sdk.schemas.logicaltypes.UuidLogicalType;
import org.apache.beam.sdk.util.BufferedElementCountingOutputStream;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.util.common.ElementByteSizeObservableIterable;
import org.apache.beam.sdk.util.common.ElementByteSizeObservableIterator;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests verifying bit-for-bit wire compatibility, byte-size observation, null handling, schema
 * evolution, single-evaluation getters, and thread safety of {@link DirectSchemaCoderGenerator},
 * {@link RowWireCodec}, and unrolled {@link RowCoderGenerator}.
 */
@RunWith(JUnit4.class)
public class DirectSchemaCoderTest {

  public enum Status {
    NEW,
    ACTIVE,
    CLOSED,
    ARCHIVED
  }

  @DefaultSchema(JavaFieldSchema.class)
  public static class NestedItem {
    public String label;
    public int qty;
    public double price;

    public NestedItem() {}

    public NestedItem(String label, int qty, double price) {
      this.label = label;
      this.qty = qty;
      this.price = price;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (!(o instanceof NestedItem)) {
        return false;
      }
      NestedItem that = (NestedItem) o;
      return qty == that.qty
          && Double.compare(price, that.price) == 0
          && Objects.equals(label, that.label);
    }

    @Override
    public int hashCode() {
      return Objects.hash(label, qty, price);
    }
  }

  @DefaultSchema(JavaFieldSchema.class)
  public static class RichPojo {
    public byte fByte;
    public short fShort;
    public int fInt;
    public long fLong;
    public float fFloat;
    public double fDouble;
    public boolean fBool;
    public String fString;
    public byte[] fBytes;
    public BigDecimal fDecimal;
    public Instant fInstant;
    public LocalDate fDate;
    public LocalTime fTime;
    public LocalDateTime fDateTime;
    public java.time.Instant fJavaInstant;
    public UUID fUuid;
    public Status fEnum;
    public List<String> fStrList;
    public List<Integer> fIntList;
    public List<Long> fLongList;
    public List<Double> fDoubleList;
    public List<NestedItem> fNestedList;
    public Map<String, Long> fStrLongMap;
    public Map<String, String> fStrStrMap;
    public Map<String, Integer> fStrIntMap;
    public Map<String, Double> fStrDblMap;
    public NestedItem fNested;

    public RichPojo() {}

    @Override
    public boolean equals(@Nullable Object o) {
      if (!(o instanceof RichPojo)) {
        return false;
      }
      RichPojo r = (RichPojo) o;
      return fByte == r.fByte
          && fShort == r.fShort
          && fInt == r.fInt
          && fLong == r.fLong
          && Float.floatToIntBits(fFloat) == Float.floatToIntBits(r.fFloat)
          && Double.doubleToLongBits(fDouble) == Double.doubleToLongBits(r.fDouble)
          && fBool == r.fBool
          && Objects.equals(fString, r.fString)
          && Arrays.equals(fBytes, r.fBytes)
          && Objects.equals(fDecimal, r.fDecimal)
          && Objects.equals(fInstant, r.fInstant)
          && Objects.equals(fDate, r.fDate)
          && Objects.equals(fTime, r.fTime)
          && Objects.equals(fDateTime, r.fDateTime)
          && Objects.equals(fJavaInstant, r.fJavaInstant)
          && Objects.equals(fUuid, r.fUuid)
          && fEnum == r.fEnum
          && Objects.equals(fStrList, r.fStrList)
          && Objects.equals(fIntList, r.fIntList)
          && Objects.equals(fLongList, r.fLongList)
          && Objects.equals(fDoubleList, r.fDoubleList)
          && Objects.equals(fNestedList, r.fNestedList)
          && Objects.equals(fStrLongMap, r.fStrLongMap)
          && Objects.equals(fStrStrMap, r.fStrStrMap)
          && Objects.equals(fStrIntMap, r.fStrIntMap)
          && Objects.equals(fStrDblMap, r.fStrDblMap)
          && Objects.equals(fNested, r.fNested);
    }

    @Override
    public int hashCode() {
      return Objects.hash(fInt, fLong, fString, fUuid);
    }
  }

  @DefaultSchema(JavaFieldSchema.class)
  public static class NullableWithDefaultsPojo {
    public long requiredId;
    public @Nullable String optStr = "non-null-default";
    public @Nullable Integer optInt = 777;
    public @Nullable Long optLong = 888L;
    public @Nullable Double optDouble = 1.25;
    public @Nullable Boolean optBool = Boolean.TRUE;
    public @Nullable UUID optUuid = new UUID(1L, 2L);
    public @Nullable Status optStatus = Status.ACTIVE;
    public @Nullable NestedItem optNested = new NestedItem("init", 1, 1.0);
    public @Nullable List<String> optList = Collections.singletonList("init");
    public @Nullable Map<String, Long> optMap = Collections.singletonMap("init", 1L);

    public NullableWithDefaultsPojo() {}

    @Override
    public boolean equals(@Nullable Object o) {
      if (!(o instanceof NullableWithDefaultsPojo)) {
        return false;
      }
      NullableWithDefaultsPojo that = (NullableWithDefaultsPojo) o;
      return requiredId == that.requiredId
          && Objects.equals(optStr, that.optStr)
          && Objects.equals(optInt, that.optInt)
          && Objects.equals(optLong, that.optLong)
          && Objects.equals(optDouble, that.optDouble)
          && Objects.equals(optBool, that.optBool)
          && Objects.equals(optUuid, that.optUuid)
          && optStatus == that.optStatus
          && Objects.equals(optNested, that.optNested)
          && Objects.equals(optList, that.optList)
          && Objects.equals(optMap, that.optMap);
    }

    @Override
    public int hashCode() {
      return Objects.hash(requiredId, optStr, optInt);
    }
  }

  @DefaultSchema(JavaBeanSchema.class)
  public static class FluentCountingBean {
    private int id;
    private @Nullable String name = "default-name";
    private @Nullable Long count;
    private List<String> tags = new ArrayList<>();
    private Map<String, Long> metrics = new HashMap<>();
    final transient AtomicInteger nameGetterCalls = new AtomicInteger();

    public int getId() {
      return id;
    }

    public FluentCountingBean setId(int id) {
      this.id = id;
      return this;
    }

    public @Nullable String getName() {
      nameGetterCalls.incrementAndGet();
      return name;
    }

    public FluentCountingBean setName(@Nullable String name) {
      this.name = name;
      return this;
    }

    public @Nullable Long getCount() {
      return count;
    }

    public void setCount(@Nullable Long count) {
      this.count = count;
    }

    public List<String> getTags() {
      return tags;
    }

    public void setTags(List<String> tags) {
      this.tags = tags;
    }

    public Map<String, Long> getMetrics() {
      return metrics;
    }

    public void setMetrics(Map<String, Long> metrics) {
      this.metrics = metrics;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (!(o instanceof FluentCountingBean)) {
        return false;
      }
      FluentCountingBean that = (FluentCountingBean) o;
      return id == that.id
          && Objects.equals(name, that.name)
          && Objects.equals(count, that.count)
          && Objects.equals(tags, that.tags)
          && Objects.equals(metrics, that.metrics);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, name, count);
    }
  }

  @DefaultSchema(JavaFieldSchema.class)
  public static class SmallPojoV2 {
    public @Nullable Integer a;
    public @Nullable String b;
    public @Nullable Long c;

    public SmallPojoV2() {}
  }

  @DefaultSchema(JavaFieldSchema.class)
  public static class OrderedPojo {
    public String alpha;
    public int beta;
    public @Nullable String gamma;
    public long delta;

    public OrderedPojo() {}
  }

  private static byte[] encode(Coder coder, Object value) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    coder.encode(value, out);
    return out.toByteArray();
  }

  private static long observeSize(Coder coder, Object value) throws Exception {
    final long[] total = {0L};
    ElementByteSizeObserver obs =
        new ElementByteSizeObserver() {
          @Override
          protected void reportElementSize(long elementSize) {
            total[0] += elementSize;
          }
        };
    coder.registerByteSizeObserver(value, obs);
    obs.advance();
    return total[0];
  }

  private static RichPojo sampleRichPojo(String s, BigDecimal dec, int i, long l) {
    RichPojo p = new RichPojo();
    p.fByte = (byte) -42;
    p.fShort = (short) -12345;
    p.fInt = i;
    p.fLong = l;
    p.fFloat = Float.NaN;
    p.fDouble = -0.0;
    p.fBool = true;
    p.fString = s;
    p.fBytes = new byte[] {0, 1, -1, 127, -128};
    p.fDecimal = dec;
    p.fInstant = Instant.ofEpochMilli(1_700_000_000_123L);
    p.fDate = LocalDate.of(2026, 11, 25);
    p.fTime = LocalTime.of(23, 59, 58, 123_456_789);
    p.fDateTime = LocalDateTime.of(1999, 12, 31, 12, 30, 45, 500);
    p.fJavaInstant = java.time.Instant.ofEpochSecond(-12345L, 987_654_321);
    p.fUuid = new UUID(0xFEDCBA9876543210L, 0x0123456789ABCDEFL);
    p.fEnum = Status.ARCHIVED;
    p.fStrList = Arrays.asList("", "ascii", "café", "🚀");
    p.fIntList = Arrays.asList(Integer.MIN_VALUE, -1, 0, 127, 128, Integer.MAX_VALUE);
    p.fLongList = Arrays.asList(Long.MIN_VALUE, -1L, 0L, 127L, 128L, Long.MAX_VALUE);
    p.fDoubleList = Arrays.asList(-0.0, 0.0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
    p.fNestedList = Arrays.asList(new NestedItem("a", 1, 2.5), new NestedItem("bé", -3, 0.0));
    p.fStrLongMap = new LinkedHashMap<>();
    p.fStrLongMap.put("k1", -1L);
    p.fStrLongMap.put("clé", Long.MAX_VALUE);
    p.fStrStrMap = new LinkedHashMap<>();
    p.fStrStrMap.put("a", "");
    p.fStrStrMap.put("b", "val-🚀");
    p.fStrIntMap = new LinkedHashMap<>();
    p.fStrIntMap.put("x", -100);
    p.fStrDblMap = new LinkedHashMap<>();
    p.fStrDblMap.put("pi", Math.PI);
    p.fNested = new NestedItem("Seattle", 98101, 47.6062);
    return p;
  }

  private static String repeatChar(char c, int count) {
    char[] chars = new char[count];
    Arrays.fill(chars, c);
    return new String(chars);
  }

  @Test
  public void testRichPojoWireIdentityWithRowCoder() throws Exception {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SchemaCoder<RichPojo> coder = registry.getSchemaCoder(RichPojo.class);
    assertTrue("Expected DirectSchemaCoder for RichPojo", coder.hasDirectCoder());
    RowCoder rowCoder = RowCoder.of(coder.getSchema());

    RichPojo[] cases =
        new RichPojo[] {
          sampleRichPojo("", BigDecimal.ZERO, 0, 0L),
          sampleRichPojo(repeatChar('a', 127), new BigDecimal("127"), 127, 127L),
          sampleRichPojo(repeatChar('b', 128), new BigDecimal("128"), 128, 128L),
          sampleRichPojo(repeatChar('c', 253), new BigDecimal("-128"), -1, -1L),
          sampleRichPojo(
              repeatChar('d', 254), new BigDecimal("-129"), Integer.MIN_VALUE, Long.MIN_VALUE),
          sampleRichPojo(repeatChar('e', 255), new BigDecimal("32767"), 16383, Long.MAX_VALUE),
          sampleRichPojo(repeatChar('f', 512), new BigDecimal("-32769"), 16384, -999_999_999_999L),
          sampleRichPojo(
              "hello café 日本語 🚀", new BigDecimal(BigInteger.valueOf(Long.MAX_VALUE), 5), -42, 42L),
          sampleRichPojo(
              "abc\uD800xyz", // unpaired high surrogate inside ASCII string
              new BigDecimal("1234567890123456789012345678901234567890.99"),
              Integer.MAX_VALUE,
              1L),
          sampleRichPojo("min-long", new BigDecimal(BigInteger.valueOf(Long.MIN_VALUE), -3), 0, 0L),
        };

    for (RichPojo p : cases) {
      byte[] directBytes = encode(coder, p);
      Row asRow = coder.getToRowFunction().apply(p);
      byte[] rowBytes = encode(rowCoder, asRow);
      assertArrayEquals("Wire mismatch for string=" + p.fString, rowBytes, directBytes);

      RichPojo decoded = coder.decode(new ByteArrayInputStream(directBytes));
      RichPojo expectedPojo = p;
      if (p.fString.indexOf('\uD800') >= 0) {
        // UTF-8 encoding replaces unpaired surrogates with U+FFFD identically to StringUtf8Coder.
        expectedPojo =
            coder.getFromRowFunction().apply(rowCoder.decode(new ByteArrayInputStream(rowBytes)));
        assertEquals("abc?xyz", decoded.fString);
      }
      assertEquals(expectedPojo, decoded);

      // Cross-decode: RowCoder bytes -> DirectSchemaCoder and DirectSchemaCoder bytes -> RowCoder.
      RichPojo fromRowBytes = coder.decode(new ByteArrayInputStream(rowBytes));
      assertEquals(expectedPojo, fromRowBytes);
      Row rowDecoded = rowCoder.decode(new ByteArrayInputStream(directBytes));
      assertEquals(expectedPojo, coder.getFromRowFunction().apply(rowDecoded));

      assertEquals(directBytes.length, observeSize(coder, p));
      assertEquals(rowBytes.length, observeSize(rowCoder, asRow));
    }

    // Empty lists & maps.
    RichPojo empty = sampleRichPojo("", BigDecimal.ONE, 1, 1L);
    empty.fBytes = new byte[0];
    empty.fStrList = Collections.emptyList();
    empty.fIntList = Collections.emptyList();
    empty.fLongList = Collections.emptyList();
    empty.fDoubleList = Collections.emptyList();
    empty.fNestedList = Collections.emptyList();
    empty.fStrLongMap = Collections.emptyMap();
    empty.fStrStrMap = Collections.emptyMap();
    empty.fStrIntMap = Collections.emptyMap();
    empty.fStrDblMap = Collections.emptyMap();
    byte[] directEmpty = encode(coder, empty);
    byte[] rowEmpty = encode(rowCoder, coder.getToRowFunction().apply(empty));
    assertArrayEquals(rowEmpty, directEmpty);
    assertEquals(empty, coder.decode(new ByteArrayInputStream(directEmpty)));
    assertEquals(directEmpty.length, observeSize(coder, empty));
  }

  @Test
  public void testNullablePojoOverwritesConstructorDefaults() throws Exception {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SchemaCoder<NullableWithDefaultsPojo> coder =
        registry.getSchemaCoder(NullableWithDefaultsPojo.class);
    assertTrue("Expected DirectSchemaCoder", coder.hasDirectCoder());
    RowCoder rowCoder = RowCoder.of(coder.getSchema());

    NullableWithDefaultsPojo allNulls = new NullableWithDefaultsPojo();
    allNulls.requiredId = 42L;
    allNulls.optStr = null;
    allNulls.optInt = null;
    allNulls.optLong = null;
    allNulls.optDouble = null;
    allNulls.optBool = null;
    allNulls.optUuid = null;
    allNulls.optStatus = null;
    allNulls.optNested = null;
    allNulls.optList = null;
    allNulls.optMap = null;

    byte[] directBytes = encode(coder, allNulls);
    byte[] rowBytes = encode(rowCoder, coder.getToRowFunction().apply(allNulls));
    assertArrayEquals(rowBytes, directBytes);
    assertEquals(directBytes.length, observeSize(coder, allNulls));

    NullableWithDefaultsPojo decoded = coder.decode(new ByteArrayInputStream(directBytes));
    assertEquals(42L, decoded.requiredId);
    assertNull(decoded.optStr);
    assertNull(decoded.optInt);
    assertNull(decoded.optLong);
    assertNull(decoded.optDouble);
    assertNull(decoded.optBool);
    assertNull(decoded.optUuid);
    assertNull(decoded.optStatus);
    assertNull(decoded.optNested);
    assertNull(decoded.optList);
    assertNull(decoded.optMap);
    assertEquals(allNulls, decoded);

    // Sparse nulls & zero nulls.
    NullableWithDefaultsPojo nonNull = new NullableWithDefaultsPojo();
    nonNull.requiredId = -9L;
    byte[] nonNullBytes = encode(coder, nonNull);
    assertArrayEquals(encode(rowCoder, coder.getToRowFunction().apply(nonNull)), nonNullBytes);
    assertEquals(nonNull, coder.decode(new ByteArrayInputStream(nonNullBytes)));
    assertEquals(nonNullBytes.length, observeSize(coder, nonNull));
  }

  @Test
  public void testFluentJavaBeanAndSingleGetterInvocation() throws Exception {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SchemaCoder<FluentCountingBean> coder = registry.getSchemaCoder(FluentCountingBean.class);
    assertTrue("Expected DirectSchemaCoder for JavaBean", coder.hasDirectCoder());
    RowCoder rowCoder = RowCoder.of(coder.getSchema());

    FluentCountingBean bean = new FluentCountingBean().setId(7).setName("hello");
    bean.setCount(null);
    bean.setTags(Arrays.asList("x", "y"));
    bean.setMetrics(ImmutableMap.of("m1", 10L));

    bean.nameGetterCalls.set(0);
    byte[] directBytes = encode(coder, bean);
    assertEquals(
        "Nullable JavaBean getter must be invoked exactly once", 1, bean.nameGetterCalls.get());

    byte[] rowBytes = encode(rowCoder, coder.getToRowFunction().apply(bean));
    assertArrayEquals(rowBytes, directBytes);
    assertEquals(directBytes.length, observeSize(coder, bean));

    FluentCountingBean decoded = coder.decode(new ByteArrayInputStream(directBytes));
    assertEquals(bean, decoded);
    assertNull(decoded.getCount());

    // Now test null name overwrites "default-name" constructor initializer.
    bean.setName(null);
    bean.setCount(99L);
    bean.nameGetterCalls.set(0);
    byte[] nullNameBytes = encode(coder, bean);
    assertEquals(1, bean.nameGetterCalls.get());
    FluentCountingBean decodedNull = coder.decode(new ByteArrayInputStream(nullNameBytes));
    assertNull(decodedNull.getName());
    assertEquals(Long.valueOf(99L), decodedNull.getCount());
    assertEquals(nullNameBytes.length, observeSize(coder, bean));
  }

  @Test
  public void testStaticEncodingAndEncodingPositionOverride() throws Exception {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    Schema baseSchema = registry.getSchema(OrderedPojo.class);
    UUID uuid = UUID.randomUUID();
    Schema reordered =
        Schema.builder()
            .addStringField("alpha")
            .addInt32Field("beta")
            .addNullableStringField("gamma")
            .addInt64Field("delta")
            .build();
    reordered.setUUID(uuid);
    RowCoder.overrideEncodingPositions(
        uuid, ImmutableMap.of("delta", 0, "gamma", 1, "alpha", 2, "beta", 3));

    JavaFieldSchema provider = new JavaFieldSchema();
    SchemaCoder<OrderedPojo> coder =
        SchemaCoder.of(
            reordered,
            org.apache.beam.sdk.values.TypeDescriptor.of(OrderedPojo.class),
            provider
            .new ToRowWithValueGetters<>(
                reordered, org.apache.beam.sdk.values.TypeDescriptor.of(OrderedPojo.class)),
            provider.fromRowFunction(TypeDescriptor.of(OrderedPojo.class)));
    assertNotNull(baseSchema);

    // Also test direct static_encoding with NestedItem.
    Schema staticSchema =
        Schema.builder()
            .addStringField("label")
            .addInt32Field("qty")
            .addDoubleField("price")
            .setOptions(
                Schema.Options.builder()
                    .setOption("beam:option:row:static_encoding", FieldType.BOOLEAN, true)
                    .build())
            .build();
    SchemaCoder<NestedItem> staticCoder =
        SchemaCoder.of(
            staticSchema,
            org.apache.beam.sdk.values.TypeDescriptor.of(NestedItem.class),
            provider
            .new ToRowWithValueGetters<>(
                staticSchema, org.apache.beam.sdk.values.TypeDescriptor.of(NestedItem.class)),
            provider.fromRowFunction(TypeDescriptor.of(NestedItem.class)));
    assertTrue("Expected DirectSchemaCoder for static encoding", staticCoder.hasDirectCoder());
    NestedItem item = new NestedItem("apple", 5, 1.5);
    byte[] directStatic = encode(staticCoder, item);
    byte[] rowStatic =
        encode(RowCoder.of(staticSchema), staticCoder.getToRowFunction().apply(item));
    assertArrayEquals(rowStatic, directStatic);
    assertEquals(item, staticCoder.decode(new ByteArrayInputStream(directStatic)));
    assertEquals(directStatic.length, observeSize(staticCoder, item));

    // Verify overrideEncodingPositions bytes match RowCoder for reordered schema.
    OrderedPojo op = new OrderedPojo();
    op.alpha = "hi";
    op.beta = 42;
    op.gamma = null;
    op.delta = 999L;
    byte[] directReordered = encode(coder, op);
    byte[] rowReordered = encode(RowCoder.of(reordered), coder.getToRowFunction().apply(op));
    assertArrayEquals(rowReordered, directReordered);
    assertTrue(coder.hasDirectCoder());
    OrderedPojo decOp = coder.decode(new ByteArrayInputStream(directReordered));
    assertEquals("hi", decOp.alpha);
    assertEquals(42, decOp.beta);
    assertNull(decOp.gamma);
    assertEquals(999L, decOp.delta);
    assertEquals(directReordered.length, observeSize(coder, op));
  }

  @Test
  public void testWideRowCoderOver64FieldsAndBigDecimalBoundaries() throws Exception {
    Schema.Builder sb = Schema.builder();
    Object[] vals = new Object[70];
    for (int i = 0; i < 70; i++) {
      sb.addNullableInt32Field("f_" + i);
      vals[i] = (i % 7 == 0) ? null : (i * -13);
    }
    Schema wide = sb.build();
    RowCoder wideCoder = RowCoder.of(wide);
    Row row = Row.withSchema(wide).addValues(vals).build();
    byte[] bytes = encode(wideCoder, row);
    assertEquals(row, wideCoder.decode(new ByteArrayInputStream(bytes)));
    assertEquals(bytes.length, observeSize(wideCoder, row));

    // Also test BigDecimal boundaries bit-for-bit against BigDecimalCoder.
    BigDecimal[] decimals = {
      BigDecimal.ZERO,
      new BigDecimal("0.00"),
      BigDecimal.ONE,
      new BigDecimal("-1"),
      new BigDecimal("127"),
      new BigDecimal("128"),
      new BigDecimal("-128"),
      new BigDecimal("-129"),
      new BigDecimal("32767"),
      new BigDecimal("32768"),
      new BigDecimal("-32768"),
      new BigDecimal("-32769"),
      BigDecimal.valueOf(Long.MAX_VALUE, 10),
      BigDecimal.valueOf(Long.MIN_VALUE, -5),
      new BigDecimal(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE), 2),
      new BigDecimal(BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE), 2),
    };
    org.apache.beam.sdk.coders.BigDecimalCoder refCoder =
        org.apache.beam.sdk.coders.BigDecimalCoder.of();
    byte[] scratch = new byte[256];
    for (BigDecimal bd : decimals) {
      ByteArrayOutputStream actual = new ByteArrayOutputStream();
      int pos = RowWireCodec.packDecimal(bd, scratch, 0, actual);
      RowWireCodec.flush(scratch, pos, actual);
      byte[] expected = encode(refCoder, bd);
      assertArrayEquals("BigDecimal wire mismatch for " + bd, expected, actual.toByteArray());
      assertEquals(bd, RowWireCodec.readDecimal(new ByteArrayInputStream(expected), scratch));
      assertEquals(expected.length, RowWireCodec.sizeOfDecimal(bd));
    }
  }

  @Test
  public void testSchemaEvolutionAndOversizedNullBitmap() throws Exception {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SchemaCoder<SmallPojoV2> coderV2 = registry.getSchemaCoder(SmallPojoV2.class);
    assertTrue(coderV2.hasDirectCoder());

    // Write a 2-field stream (older schema missing field 'c').
    Schema v1Schema =
        Schema.builder().addNullableInt32Field("a").addNullableStringField("b").build();
    Row v1Row = Row.withSchema(v1Schema).addValues(123, "old").build();
    byte[] v1Bytes = encode(RowCoder.of(v1Schema), v1Row);

    SmallPojoV2 decoded = coderV2.decode(new ByteArrayInputStream(v1Bytes));
    assertEquals(Integer.valueOf(123), decoded.a);
    assertEquals("old", decoded.b);
    assertNull(decoded.c);

    // Oversized null bitmap (10 bytes all zero except bit 1 => 'b' is null).
    ByteArrayOutputStream padded = new ByteArrayOutputStream();
    VarInt.encode(3, padded);
    VarInt.encode(10, padded);
    byte[] nullBytes = new byte[10];
    nullBytes[0] = 0x02; // field 1 ('b') is null
    padded.write(nullBytes);
    VarInt.encode(77, padded); // a = 77
    VarInt.encode(555L, padded); // c = 555
    SmallPojoV2 decodedPadded = coderV2.decode(new ByteArrayInputStream(padded.toByteArray()));
    assertEquals(Integer.valueOf(77), decodedPadded.a);
    assertNull(decodedPadded.b);
    assertEquals(Long.valueOf(555L), decodedPadded.c);
  }

  @Test
  public void testChunkedUnknownSizeIterableDecoding() throws Exception {
    byte[] scratch = new byte[256];
    // Emit a ListCoder stream using unknown-size (-1) chunked framing.
    ByteArrayOutputStream strStream = new ByteArrayOutputStream();
    strStream.write(new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff});
    BufferedElementCountingOutputStream counting =
        new BufferedElementCountingOutputStream(strStream);
    for (String s : Arrays.asList("one", "two", "three")) {
      counting.markElementStart();
      StringUtf8Coder.of().encode(s, counting);
    }
    counting.finish();
    assertEquals(
        Arrays.asList("one", "two", "three"),
        RowWireCodec.readStringList(new ByteArrayInputStream(strStream.toByteArray()), scratch));

    // Ensure IterableCoder reference gives the exact same bytes.
    Iterable<Integer> nonCollection = () -> Arrays.asList(10, -20, 30).iterator();
    ByteArrayOutputStream intStream = new ByteArrayOutputStream();
    IterableCoder.of(VarIntCoder.of()).encode(nonCollection, intStream);
    assertEquals(
        Arrays.asList(10, -20, 30),
        RowWireCodec.readInt32List(new ByteArrayInputStream(intStream.toByteArray()), scratch));
  }

  @Test
  public void testErrorCasesAndSerialization() throws Exception {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SchemaCoder<NestedItem> coder = registry.getSchemaCoder(NestedItem.class);
    assertTrue(coder.hasDirectCoder());

    // Round-trip Java serialization of SchemaCoder clears transient directCoder & restores
    // seamlessly.
    SchemaCoder<NestedItem> cloned = SerializableUtils.clone(coder);
    assertEquals(coder, cloned);
    assertEquals(coder.hashCode(), cloned.hashCode());
    NestedItem item = new NestedItem("box", 3, 9.99);
    byte[] bytes = encode(cloned, item);
    assertEquals(item, cloned.decode(new ByteArrayInputStream(bytes)));
    assertTrue(cloned.hasDirectCoder());

    // Null top-level value throws CoderException.
    assertThrows(CoderException.class, () -> coder.encode(null, new ByteArrayOutputStream()));

    // Null in non-nullable field throws RuntimeException matching RowWithGetters.
    NestedItem nullLabel = new NestedItem(null, 1, 2.0);
    RuntimeException ex =
        assertThrows(
            RuntimeException.class, () -> coder.encode(nullLabel, new ByteArrayOutputStream()));
    assertTrue(ex.getMessage().contains("Null value set on non-nullable field"));

    // Truncated stream throws CoderException / IOException.
    byte[] truncated = Arrays.copyOf(bytes, bytes.length - 2);
    assertThrows(IOException.class, () -> coder.decode(new ByteArrayInputStream(truncated)));

    // Empty collection vs null-element checks.
    assertFalse(coder.isRegisterByteSizeObserverCheap(null));
    assertTrue(coder.isRegisterByteSizeObserverCheap(item));
    NestedItem longStr = new NestedItem(repeatChar('z', 2000), 1, 2.0);
    assertFalse(coder.isRegisterByteSizeObserverCheap(longStr));

    // Invalid enum ordinal check.
    assertThrows(
        IllegalArgumentException.class,
        () -> DirectSchemaCoderGenerator.enumConstantForOrdinal(Status.class, -1));
    assertThrows(
        IllegalArgumentException.class,
        () -> DirectSchemaCoderGenerator.enumConstantForOrdinal(Status.class, 99));

    // Null elements inside non-nullable list/map throw CoderException identically.
    byte[] scratch = new byte[256];
    assertThrows(
        CoderException.class,
        () ->
            RowWireCodec.packStringCollection(
                Arrays.asList("ok", null), scratch, 0, new ByteArrayOutputStream()));
    Map<String, Long> nullValMap = new HashMap<>();
    nullValMap.put("k", null);
    assertThrows(
        CoderException.class,
        () -> RowWireCodec.packStringLongMap(nullValMap, scratch, 0, new ByteArrayOutputStream()));
  }

  @Test
  public void testLogicalTypeZeroRoundTripIdentityInRowAndCreator() throws Exception {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SchemaCoder<RichPojo> coder = registry.getSchemaCoder(RichPojo.class);
    RichPojo original = sampleRichPojo("id-test", BigDecimal.TEN, 1, 2L);

    // toRow (RowWithGetters) passes exact immutable logical-type references straight through.
    Row lazyRow = coder.getToRowFunction().apply(original);
    assertSame(original.fUuid, lazyRow.getLogicalTypeValue("fUuid", UUID.class));
    assertSame(original.fDate, lazyRow.getLogicalTypeValue("fDate", LocalDate.class));
    assertSame(original.fTime, lazyRow.getLogicalTypeValue("fTime", LocalTime.class));
    assertSame(original.fDateTime, lazyRow.getLogicalTypeValue("fDateTime", LocalDateTime.class));
    assertSame(
        original.fJavaInstant,
        lazyRow.getLogicalTypeValue("fJavaInstant", java.time.Instant.class));

    // Materialize a RowWithStorage holding those exact references and verify FromRowUsingCreator
    // avoids converting toBaseType -> toInputType.
    Object[] materialized = new Object[coder.getSchema().getFieldCount()];
    for (int i = 0; i < materialized.length; i++) {
      materialized[i] = lazyRow.getValue(i);
    }
    Row storageRow = Row.attachValues(coder.getSchema(), materialized);
    RichPojo recreated = coder.getFromRowFunction().apply(storageRow);
    assertSame(original.fUuid, recreated.fUuid);
    assertSame(original.fDate, recreated.fDate);
    assertSame(original.fTime, recreated.fTime);
    assertSame(original.fDateTime, recreated.fDateTime);
    assertSame(original.fJavaInstant, recreated.fJavaInstant);
  }

  private static List<Object> nonRandomAccessList(Object... values) {
    return new AbstractList<Object>() {
      @Override
      public Object get(int index) {
        return values[index];
      }

      @Override
      public int size() {
        return values.length;
      }
    };
  }

  @Test
  public void testRowFieldCodecActiveAndStorageVariantsWireIdentical() throws Exception {
    Schema nonNullSchema =
        Schema.builder()
            .addInt32Field("a")
            .addInt64Field("b")
            .addDoubleField("c")
            .addStringField("d")
            .build();
    RowCoder nonNullCoder = RowCoder.of(nonNullSchema);
    assertTrue(
        "Expected generated RowFieldCodec for nonNullSchema",
        RowCoderGenerator.hasGeneratedFieldCodec(RowCoderGenerator.generate(nonNullSchema)));

    Object[] vals = new Object[] {42, 9_876_543_210L, 3.14, "hello"};
    Row builderRow = Row.withSchema(nonNullSchema).addValues(vals).build();
    Row arrayRow = Row.attachValues(nonNullSchema, vals);
    Row nonRaRow = Row.attachValues(nonNullSchema, nonRandomAccessList(vals));
    byte[] builderBytes = encode(nonNullCoder, builderRow);
    assertArrayEquals(builderBytes, encode(nonNullCoder, arrayRow));
    assertArrayEquals(builderBytes, encode(nonNullCoder, nonRaRow));
    assertEquals(builderRow, nonNullCoder.decode(new ByteArrayInputStream(builderBytes)));

    Schema nullableSchema =
        Schema.builder()
            .addInt32Field("a")
            .addNullableStringField("b")
            .addNullableInt64Field("c")
            .addDoubleField("d")
            .build();
    RowCoder nullableCoder = RowCoder.of(nullableSchema);
    assertTrue(
        "Expected generated RowFieldCodec for nullableSchema",
        RowCoderGenerator.hasGeneratedFieldCodec(RowCoderGenerator.generate(nullableSchema)));

    Object[] nullableVals = new Object[] {7, null, 123L, 2.5};
    Row nullableBuilderRow = Row.withSchema(nullableSchema).addValues(nullableVals).build();
    Row nullableArrayRow = Row.attachValues(nullableSchema, nullableVals);
    Row nullableNonRaRow = Row.attachValues(nullableSchema, nonRandomAccessList(nullableVals));
    byte[] nullableBytes = encode(nullableCoder, nullableBuilderRow);
    assertArrayEquals(nullableBytes, encode(nullableCoder, nullableArrayRow));
    assertArrayEquals(nullableBytes, encode(nullableCoder, nullableNonRaRow));
    assertEquals(nullableBuilderRow, nullableCoder.decode(new ByteArrayInputStream(nullableBytes)));
    assertEquals(nullableBytes.length, observeSize(nullableCoder, nullableBuilderRow));
    assertEquals(nullableBytes.length, observeSize(nullableCoder, nullableArrayRow));

    // 65..1024 fields uses a chunked generated RowFieldCodec (single-method cutoff stays at 64).
    Schema.Builder wideBuilder = Schema.builder();
    Object[] wideVals = new Object[70];
    for (int i = 0; i < 70; i++) {
      wideBuilder.addNullableStringField("f" + i);
      wideVals[i] = (i % 7 == 0) ? null : ("v" + i);
    }
    Schema wideSchema = wideBuilder.build();
    RowCoder wideCoder = RowCoder.of(wideSchema);
    assertTrue(RowCoderGenerator.hasGeneratedFieldCodec(RowCoderGenerator.generate(wideSchema)));
    Row wideBuilderRow = Row.withSchema(wideSchema).addValues(wideVals).build();
    Row wideArrayRow = Row.attachValues(wideSchema, wideVals);
    byte[] wideBytes = encode(wideCoder, wideBuilderRow);
    assertArrayEquals(wideBytes, encode(wideCoder, wideArrayRow));
    assertEquals(wideBuilderRow, wideCoder.decode(new ByteArrayInputStream(wideBytes)));
  }

  private static class HiddenBase {
    public String baseName;
  }

  @DefaultSchema(JavaFieldSchema.class)
  public static class PublicSubOfHiddenBase extends HiddenBase {
    public int count;

    public PublicSubOfHiddenBase() {}

    public PublicSubOfHiddenBase(String baseName, int count) {
      this.baseName = baseName;
      this.count = count;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (!(o instanceof PublicSubOfHiddenBase)) {
        return false;
      }
      PublicSubOfHiddenBase that = (PublicSubOfHiddenBase) o;
      return count == that.count && Objects.equals(baseName, that.baseName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(baseName, count);
    }
  }

  private enum HiddenEnum {
    ALPHA,
    BETA
  }

  @DefaultSchema(JavaFieldSchema.class)
  public static class PojoWithPrivateEnum {
    public HiddenEnum kind;
    public int value;

    public PojoWithPrivateEnum() {}

    public PojoWithPrivateEnum(HiddenEnum kind, int value) {
      this.kind = kind;
      this.value = value;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (!(o instanceof PojoWithPrivateEnum)) {
        return false;
      }
      PojoWithPrivateEnum that = (PojoWithPrivateEnum) o;
      return kind == that.kind && value == that.value;
    }

    @Override
    public int hashCode() {
      return Objects.hash(kind, value);
    }
  }

  @DefaultSchema(JavaBeanSchema.class)
  public static class ReentrantBean {
    private static final Schema INNER_SCHEMA =
        Schema.builder().addStringField("k").addInt64Field("v").build();
    private static final RowCoder INNER_CODER = RowCoder.of(INNER_SCHEMA);

    private String prefix;
    private String innerTag;
    private long suffix;

    public ReentrantBean() {}

    public ReentrantBean(String prefix, String innerTag, long suffix) {
      this.prefix = prefix;
      this.innerTag = innerTag;
      this.suffix = suffix;
    }

    public String getPrefix() {
      return prefix;
    }

    public void setPrefix(String prefix) {
      this.prefix = prefix;
    }

    public String getInnerTag() {
      // Trigger nested RowCoder.encode on the same thread mid-encoding to verify re-entrant
      // ScratchPool safety.
      try {
        Row inner = Row.withSchema(INNER_SCHEMA).addValues("reentrant-" + innerTag, 999L).build();
        ByteArrayOutputStream innerOut = new ByteArrayOutputStream();
        INNER_CODER.encode(inner, innerOut);
        Row decoded = INNER_CODER.decode(new ByteArrayInputStream(innerOut.toByteArray()));
        if (!inner.equals(decoded)) {
          throw new IllegalStateException("Reentrant inner round-trip mismatch");
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return innerTag;
    }

    public void setInnerTag(String innerTag) {
      this.innerTag = innerTag;
    }

    public long getSuffix() {
      return suffix;
    }

    public void setSuffix(long suffix) {
      this.suffix = suffix;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (!(o instanceof ReentrantBean)) {
        return false;
      }
      ReentrantBean that = (ReentrantBean) o;
      return suffix == that.suffix
          && Objects.equals(prefix, that.prefix)
          && Objects.equals(innerTag, that.innerTag);
    }

    @Override
    public int hashCode() {
      return Objects.hash(prefix, innerTag, suffix);
    }
  }

  private static final class CountingObservableCollection<T>
      extends ElementByteSizeObservableIterable<T, ElementByteSizeObservableIterator<T>>
      implements java.util.Collection<T> {
    private final List<T> elements;
    final AtomicInteger createIteratorCalls = new AtomicInteger();

    CountingObservableCollection(List<T> elements) {
      this.elements = elements;
    }

    @Override
    protected ElementByteSizeObservableIterator<T> createIterator() {
      createIteratorCalls.incrementAndGet();
      Iterator<T> it = elements.iterator();
      return new ElementByteSizeObservableIterator<T>() {
        @Override
        public boolean hasNext() {
          return it.hasNext();
        }

        @Override
        public T next() {
          T v = it.next();
          notifyValueReturned(16L);
          return v;
        }
      };
    }

    @Override
    public int size() {
      return elements.size();
    }

    @Override
    public boolean isEmpty() {
      return elements.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
      return elements.contains(o);
    }

    @Override
    public Object[] toArray() {
      return elements.toArray();
    }

    @Override
    public <ElemT> ElemT[] toArray(ElemT[] a) {
      return elements.toArray(a);
    }

    @Override
    public boolean add(T e) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(java.util.Collection<?> c) {
      return elements.containsAll(c);
    }

    @Override
    public boolean addAll(java.util.Collection<? extends T> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(java.util.Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(java.util.Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException();
    }
  }

  @DefaultSchema(JavaFieldSchema.class)
  public static class ObservableIterablePojo {
    public java.util.Collection<String> tags;
    public java.util.Collection<NestedItem> items;

    public ObservableIterablePojo() {}
  }

  @Test
  public void testReentrantEncodeDoesNotCorruptScratchBuffer() throws Exception {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SchemaCoder<ReentrantBean> coder = registry.getSchemaCoder(ReentrantBean.class);
    assertTrue(coder.hasDirectCoder());
    ReentrantBean bean = new ReentrantBean("outer-prefix-abcdef", "mid-payload", 0x123456789L);
    byte[] directBytes = encode(coder, bean);
    RowCoder rowCoder = RowCoder.of(coder.getSchema());
    byte[] rowBytes = encode(rowCoder, coder.getToRowFunction().apply(bean));
    assertArrayEquals(rowBytes, directBytes);
    assertEquals(bean, coder.decode(new ByteArrayInputStream(directBytes)));
  }

  @Test
  public void testNonPublicSuperclassAndPrivateEnumFallbackGracefully() throws Exception {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SchemaCoder<PublicSubOfHiddenBase> subCoder =
        registry.getSchemaCoder(PublicSubOfHiddenBase.class);
    PublicSubOfHiddenBase sub = new PublicSubOfHiddenBase("inherited", 42);
    byte[] subBytes = encode(subCoder, sub);
    assertEquals(sub, subCoder.decode(new ByteArrayInputStream(subBytes)));
    assertArrayEquals(
        encode(RowCoder.of(subCoder.getSchema()), subCoder.getToRowFunction().apply(sub)),
        subBytes);

    SchemaCoder<PojoWithPrivateEnum> enumCoder = registry.getSchemaCoder(PojoWithPrivateEnum.class);
    PojoWithPrivateEnum pe = new PojoWithPrivateEnum(HiddenEnum.BETA, 7);
    byte[] enumBytes = encode(enumCoder, pe);
    assertEquals(pe, enumCoder.decode(new ByteArrayInputStream(enumBytes)));
    assertArrayEquals(
        encode(RowCoder.of(enumCoder.getSchema()), enumCoder.getToRowFunction().apply(pe)),
        enumBytes);
  }

  @Test
  public void testDirectSchemaCoderLazyObservableIterable() throws Exception {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SchemaCoder<ObservableIterablePojo> coder =
        registry.getSchemaCoder(ObservableIterablePojo.class);
    assertTrue("Expected DirectSchemaCoder for ObservableIterablePojo", coder.hasDirectCoder());
    ObservableIterablePojo pojo = new ObservableIterablePojo();
    CountingObservableCollection<String> tags =
        new CountingObservableCollection<>(Arrays.asList("a", "b", "c"));
    CountingObservableCollection<NestedItem> items =
        new CountingObservableCollection<>(
            Arrays.asList(new NestedItem("x", 1, 1.0), new NestedItem("y", 2, 2.0)));
    pojo.tags = tags;
    pojo.items = items;

    assertTrue(coder.isRegisterByteSizeObserverCheap(pojo));
    ElementByteSizeObserver obs =
        new ElementByteSizeObserver() {
          @Override
          protected void reportElementSize(long elementByteSize) {}
        };
    coder.registerByteSizeObserver(pojo, obs);
    assertTrue("Expected lazy byte-size observation", obs.getIsLazy());
    assertEquals("Should not iterate tags eagerly", 0, tags.createIteratorCalls.get());
    assertEquals("Should not iterate items eagerly", 0, items.createIteratorCalls.get());
  }

  @Test
  public void testGeneratedRowCoderAndSetterCreatorSerialization() throws Exception {
    Schema schema =
        Schema.builder().addInt32Field("a").addNullableStringField("b").addInt64Field("c").build();
    Coder<Row> generated = RowCoderGenerator.generate(schema);
    assertTrue(RowCoderGenerator.hasGeneratedFieldCodec(generated));
    Coder<Row> cloned = SerializableUtils.clone(generated);
    assertTrue(RowCoderGenerator.hasGeneratedFieldCodec(cloned));
    Row row = Row.withSchema(schema).addValues(12, "hi", 34L).build();
    byte[] bytes = encode(cloned, row);
    assertArrayEquals(encode(generated, row), bytes);
    assertEquals(row, cloned.decode(new ByteArrayInputStream(bytes)));

    Schema beanSchema = SchemaRegistry.createDefault().getSchema(FluentCountingBean.class);
    SetterBasedCreatorFactory creatorFactory =
        new SetterBasedCreatorFactory(
            (td, s) ->
                org.apache.beam.sdk.schemas.utils.JavaBeanUtils.getSetters(
                    td,
                    s,
                    new JavaBeanSchema.SetterTypeSupplier(),
                    org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.DefaultTypeConversionsFactory
                        .INSTANCE));
    SchemaUserTypeCreator creator =
        creatorFactory.create(TypeDescriptor.of(FluentCountingBean.class), beanSchema);
    SchemaUserTypeCreator clonedCreator = SerializableUtils.clone(creator);
    Object[] params = new Object[beanSchema.getFieldCount()];
    params[beanSchema.indexOf("id")] = 5;
    params[beanSchema.indexOf("name")] = "cloned";
    params[beanSchema.indexOf("count")] = 99L;
    params[beanSchema.indexOf("tags")] = Collections.singletonList("t");
    params[beanSchema.indexOf("metrics")] = Collections.singletonMap("k", 1L);
    Object created = clonedCreator.create(params);
    FluentCountingBean bean = (FluentCountingBean) created;
    assertEquals(5, bean.getId());
    assertEquals("cloned", bean.getName());
  }

  @Test
  public void testRowCoderNestedSchemaWithNullUuid() throws Exception {
    Schema inner = Schema.builder().addStringField("s").addInt32Field("n").build();
    inner.setUUID(null);
    Schema outer =
        Schema.builder()
            .addRowField("sub", inner)
            .addArrayField("list", FieldType.row(inner))
            .build();
    RowCoder coder = RowCoder.of(outer);
    Row subRow = Row.withSchema(inner).addValues("ok", 1).build();
    Row outerRow =
        Row.withSchema(outer).addValues(subRow, Collections.singletonList(subRow)).build();
    byte[] bytes = encode(coder, outerRow);
    assertEquals(outerRow, coder.decode(new ByteArrayInputStream(bytes)));
  }

  @Test
  public void testNonDirectSchemaCoderNullByteSizeCheap() {
    Schema schema = Schema.builder().addStringField("s").build();
    SchemaCoder<String> customCoder =
        SchemaCoder.of(
            schema,
            TypeDescriptor.of(String.class),
            s -> Row.withSchema(schema).addValues(s).build(),
            r -> r.getString(0));
    assertFalse(customCoder.hasDirectCoder());
    assertFalse(customCoder.isRegisterByteSizeObserverCheap(null));
  }

  @Test
  public void testOneOfTypeCoercionAndValidation() {
    OneOfType oneOf =
        OneOfType.create(
            Schema.Field.of("bytes", FieldType.BYTES),
            Schema.Field.of("dt", FieldType.DATETIME),
            Schema.Field.of("num", FieldType.INT64));
    Row bytesRow =
        oneOf.toBaseType(oneOf.createValue("bytes", ByteBuffer.wrap(new byte[] {1, 2, 3})));
    assertArrayEquals(new byte[] {1, 2, 3}, bytesRow.getBytes("bytes"));

    DateTime nonUtc = new DateTime(1_700_000_000_000L, DateTimeZone.forOffsetHours(-5));
    Row dtRow = oneOf.toBaseType(oneOf.createValue("dt", nonUtc));
    assertEquals(nonUtc.toInstant(), dtRow.getDateTime("dt").toInstant());

    assertThrows(
        RuntimeException.class, () -> oneOf.toBaseType(oneOf.createValue("num", "not-a-long")));
  }

  @Test
  public void testReadNullMaskBoundsValidation() {
    byte[] buf = new byte[16];
    assertThrows(
        CoderException.class,
        () -> RowWireCodec.readNullMask(new ByteArrayInputStream(new byte[16]), -1, buf));
    assertThrows(
        CoderException.class,
        () -> RowWireCodec.readNullMask(new ByteArrayInputStream(new byte[16]), 9, buf));
  }

  @Test
  public void testConcurrentThreadSafetyOfScratchBuffer() throws Exception {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SchemaCoder<RichPojo> coder = registry.getSchemaCoder(RichPojo.class);
    RowCoder rowCoder = RowCoder.of(coder.getSchema());

    int threads = 8;
    int iterations = 1000;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      List<Future<Void>> futures = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        final int tid = t;
        futures.add(
            pool.submit(
                (Callable<Void>)
                    () -> {
                      for (int i = 0; i < iterations; i++) {
                        RichPojo p =
                            sampleRichPojo(
                                (i % 3 == 0 ? "ascii-" : "café-") + tid + "-" + i,
                                BigDecimal.valueOf((long) tid * 1000 - i, i % 5),
                                tid * 100 - i,
                                ((long) i << 20) ^ tid);
                        byte[] direct = encode(coder, p);
                        byte[] expected = encode(rowCoder, coder.getToRowFunction().apply(p));
                        assertArrayEquals(expected, direct);
                        RichPojo decoded = coder.decode(new ByteArrayInputStream(direct));
                        assertEquals(p, decoded);
                      }
                      return null;
                    }));
      }
      for (Future<Void> f : futures) {
        f.get();
      }
    } finally {
      pool.shutdownNow();
    }
  }

  /**
   * 140-field POJO exercising both chunked DirectSchemaCoder (>64) and chunked POJOUtils (>128).
   */
  @DefaultSchema(JavaFieldSchema.class)
  public static class Wide140Pojo {
    public int i00, i01, i02, i03, i04, i05, i06, i07, i08, i09;
    public int i10, i11, i12, i13, i14, i15, i16, i17, i18, i19;
    public int i20, i21, i22, i23, i24, i25, i26, i27, i28, i29;
    public int i30, i31, i32, i33, i34;
    public long l00, l01, l02, l03, l04, l05, l06, l07, l08, l09;
    public long l10, l11, l12, l13, l14, l15, l16, l17, l18, l19;
    public long l20, l21, l22, l23, l24, l25, l26, l27, l28, l29;
    public long l30, l31, l32, l33, l34;
    public double d00, d01, d02, d03, d04, d05, d06, d07, d08, d09;
    public double d10, d11, d12, d13, d14, d15, d16, d17, d18, d19;
    public double d20, d21, d22, d23, d24, d25, d26, d27, d28, d29;
    public double d30, d31, d32, d33, d34;
    public @Nullable String s00, s01, s02, s03, s04, s05, s06, s07, s08, s09;
    public @Nullable String s10, s11, s12, s13, s14, s15, s16, s17, s18, s19;
    public @Nullable String s20, s21, s22, s23, s24, s25, s26, s27, s28, s29;
    public @Nullable String s30, s31, s32, s33, s34;

    public Wide140Pojo() {
      // Non-null default to verify chunked decode explicitly overwrites defaulted fields with null.
      this.s01 = "default-non-null";
      this.s34 = "default-tail";
    }

    static Wide140Pojo sample(boolean sparseNulls) {
      Wide140Pojo p = new Wide140Pojo();
      p.i00 = 1;
      p.i17 = 17;
      p.i34 = 34;
      p.l00 = 100L;
      p.l17 = 1700L;
      p.l34 = 3400L;
      p.d00 = 0.5;
      p.d17 = 17.5;
      p.d34 = 34.5;
      p.s00 = "first";
      p.s01 = sparseNulls ? null : "second";
      p.s17 = "mid-café";
      p.s20 = sparseNulls ? null : "twenty";
      p.s33 = "thirty-three";
      p.s34 = sparseNulls ? null : "last";
      return p;
    }
  }

  private static Schema buildWideSchema(int fieldCount, boolean nullable, boolean staticEncoding) {
    Schema.Builder b = Schema.builder();
    for (int i = 0; i < fieldCount; i++) {
      FieldType ft;
      switch (i % 8) {
        case 0:
          ft = FieldType.INT32;
          break;
        case 1:
          ft = FieldType.INT64;
          break;
        case 2:
          ft = FieldType.DOUBLE;
          break;
        case 3:
          ft = FieldType.BOOLEAN;
          break;
        case 4:
          ft = FieldType.STRING;
          break;
        case 5:
          ft = FieldType.DATETIME;
          break;
        case 6:
          ft = FieldType.BYTES;
          break;
        default:
          ft = FieldType.FLOAT;
          break;
      }
      if (nullable) {
        ft = ft.withNullable(true);
      }
      b.addField("f_" + i, ft);
    }
    if (staticEncoding) {
      b.setOptions(
          Schema.Options.builder()
              .setOption("beam:option:row:static_encoding", FieldType.BOOLEAN, true)
              .build());
    }
    return b.build();
  }

  private static Row buildWideRow(Schema schema, boolean sparseNulls, boolean arrayBacked) {
    int n = schema.getFieldCount();
    Object[] arr = new Object[n];
    for (int i = 0; i < n; i++) {
      if (sparseNulls && (i % 3 == 0 || i == n - 1)) {
        arr[i] = null;
        continue;
      }
      switch (i % 8) {
        case 0:
          arr[i] = 1000 + i;
          break;
        case 1:
          arr[i] = 1_000_000_000L + i;
          break;
        case 2:
          arr[i] = 3.25 + i;
          break;
        case 3:
          arr[i] = (i & 1) == 0;
          break;
        case 4:
          arr[i] = "val-" + i;
          break;
        case 5:
          arr[i] = Instant.ofEpochMilli(1_700_000_000_000L + i);
          break;
        case 6:
          arr[i] = ("b-" + i).getBytes(StandardCharsets.UTF_8);
          break;
        default:
          arr[i] = 1.5f + i;
          break;
      }
    }
    return arrayBacked
        ? Row.attachValues(schema, arr)
        : Row.withSchema(schema).attachValues(new ArrayList<>(Arrays.asList(arr)));
  }

  @Test
  public void testChunkedRowCoder300FieldsRoundTripAndEvolution() throws Exception {
    for (boolean nullable : new boolean[] {false, true}) {
      Schema schema = buildWideSchema(300, nullable, false);
      Coder<Row> coder = RowCoderGenerator.generate(schema);
      assertTrue(
          "Expected chunked generated field codec for 300-field schema",
          RowCoderGenerator.hasGeneratedFieldCodec(coder));

      // Verify Java serialization preserves chunked codec.
      Coder<Row> cloned = SerializableUtils.clone(coder);
      assertTrue(RowCoderGenerator.hasGeneratedFieldCodec(cloned));

      for (boolean arrayBacked : new boolean[] {true, false}) {
        Row nonNullRow = buildWideRow(schema, false, arrayBacked);
        byte[] bytes = encode(cloned, nonNullRow);
        assertEquals(nonNullRow, cloned.decode(new ByteArrayInputStream(bytes)));
        assertEquals(bytes.length, observeSize(cloned, nonNullRow));

        if (nullable) {
          Row sparseRow = buildWideRow(schema, true, arrayBacked);
          byte[] sparseBytes = encode(cloned, sparseRow);
          assertEquals(sparseRow, cloned.decode(new ByteArrayInputStream(sparseBytes)));
          assertEquals(sparseBytes.length, observeSize(cloned, sparseRow));

          // All-null row exercises full 38-byte bitmap.
          Row allNullRow = Row.attachValues(schema, new Object[300]);
          byte[] allNullBytes = encode(cloned, allNullRow);
          assertEquals(allNullRow, cloned.decode(new ByteArrayInputStream(allNullBytes)));
          assertEquals(allNullBytes.length, observeSize(cloned, allNullRow));
        }
      }
    }

    // Static encoding 300-field row.
    Schema staticSchema = buildWideSchema(300, false, true);
    Coder<Row> staticCoder = RowCoderGenerator.generate(staticSchema);
    assertTrue(RowCoderGenerator.hasGeneratedFieldCodec(staticCoder));
    Row staticRow = buildWideRow(staticSchema, false, true);
    byte[] staticBytes = encode(staticCoder, staticRow);
    assertEquals(staticRow, staticCoder.decode(new ByteArrayInputStream(staticBytes)));
    assertEquals(staticBytes.length, observeSize(staticCoder, staticRow));

    // Schema evolution: stream written with 200 fields decoded by 300-field schema (nullable).
    Schema s200 = buildWideSchema(200, true, false);
    Schema s300 = buildWideSchema(300, true, false);
    Row row200 = buildWideRow(s200, true, true);
    byte[] bytes200 = encode(RowCoder.of(s200), row200);
    Row decoded300 = RowCoder.of(s300).decode(new ByteArrayInputStream(bytes200));
    for (int i = 0; i < 200; i++) {
      if (s200.getField(i).getType().getTypeName() == Schema.TypeName.BYTES) {
        assertArrayEquals(row200.getBytes(i), decoded300.getBytes(i));
      } else {
        Object expectedVal = row200.getValue(i);
        Object actualVal = decoded300.getValue(i);
        assertEquals(expectedVal, actualVal);
      }
    }
    for (int i = 200; i < 300; i++) {
      assertNull(decoded300.getValue(i));
    }
  }

  @Test
  public void testVeryWideRowCoder1100FieldsLoopFallback() throws Exception {
    Schema schema = buildWideSchema(1100, true, false);
    Coder<Row> coder = RowCoderGenerator.generate(schema);
    assertFalse(
        "Schemas above 1024 fields should use the buffered loop fallback",
        RowCoderGenerator.hasGeneratedFieldCodec(coder));
    Row row = buildWideRow(schema, true, true);
    byte[] bytes = encode(coder, row);
    assertEquals(row, coder.decode(new ByteArrayInputStream(bytes)));
    assertEquals(bytes.length, observeSize(coder, row));
  }

  @Test
  public void testChunkedDirectSchemaCoderAndChunkedPojoCreator140Fields() throws Exception {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SchemaCoder<Wide140Pojo> coder = registry.getSchemaCoder(Wide140Pojo.class);
    assertEquals(140, coder.getSchema().getFieldCount());
    assertTrue("Expected chunked DirectSchemaCoder for Wide140Pojo", coder.hasDirectCoder());

    RowCoder rowCoder = RowCoder.of(coder.getSchema());
    for (boolean sparseNulls : new boolean[] {false, true}) {
      Wide140Pojo sample = Wide140Pojo.sample(sparseNulls);
      byte[] directBytes = encode(coder, sample);
      Row asRow = coder.getToRowFunction().apply(sample);
      byte[] rowBytes = encode(rowCoder, asRow);
      assertArrayEquals(rowBytes, directBytes);

      assertTrue(coder.isRegisterByteSizeObserverCheap(sample));
      assertEquals(directBytes.length, observeSize(coder, sample));

      Wide140Pojo decodedDirect = coder.decode(new ByteArrayInputStream(directBytes));
      assertEquals(asRow, coder.getToRowFunction().apply(decodedDirect));

      // Also exercise chunked POJOUtils.SetFieldCreateInstruction (>128 fields) via
      // fromRowFunction.
      Row decodedRow = rowCoder.decode(new ByteArrayInputStream(rowBytes));
      Wide140Pojo fromRow = coder.getFromRowFunction().apply(decodedRow);
      assertEquals(asRow, coder.getToRowFunction().apply(fromRow));
    }
  }

  @Test
  public void testRowWithGettersCompactContainerCacheOnWideSchema() {
    Schema.Builder b = Schema.builder();
    for (int i = 0; i < 300; i++) {
      if (i == 5) {
        b.addNullableArrayField("list_5", FieldType.STRING);
      } else if (i == 250) {
        b.addNullableMapField("map_250", FieldType.STRING, FieldType.INT64);
      } else {
        b.addInt32Field("f_" + i);
      }
    }
    Schema schema = b.build();
    assertEquals(2, schema.getCacheableContainerCount());
    assertEquals(0, schema.getContainerCacheSlot(5));
    assertEquals(1, schema.getContainerCacheSlot(250));
    assertEquals(-1, schema.getContainerCacheSlot(0));
    assertEquals(-1, schema.getContainerCacheSlot(100));
    assertEquals(-1, schema.getContainerCacheSlot(299));

    AtomicInteger listCalls = new AtomicInteger();
    AtomicInteger mapCalls = new AtomicInteger();
    List<FieldValueGetter<Object, Object>> getters = new ArrayList<>(300);
    for (int i = 0; i < 300; i++) {
      final int idx = i;
      getters.add(
          new FieldValueGetter<Object, Object>() {
            @Override
            public Object get(Object object) {
              if (idx == 5) {
                listCalls.incrementAndGet();
                return Arrays.asList("a", "b");
              }
              if (idx == 250) {
                mapCalls.incrementAndGet();
                return null; // exercises NULL_SENTINEL caching in compact slot 1
              }
              return idx;
            }

            @Override
            public String name() {
              return schema.getField(idx).getName();
            }
          });
    }

    Row row =
        Row.withFieldValueGetters(schema, getters, new Object(), TypeDescriptor.of(Object.class));
    assertEquals(Integer.valueOf(42), row.getInt32(42));
    assertEquals(Arrays.asList("a", "b"), new ArrayList<>(Objects.requireNonNull(row.getArray(5))));
    assertEquals(Arrays.asList("a", "b"), new ArrayList<>(Objects.requireNonNull(row.getArray(5))));
    assertNull(row.getMap(250));
    assertNull(row.getMap(250));
    assertEquals("Container getter at index 5 should be cached", 1, listCalls.get());
    assertEquals("Null container getter at index 250 should be cached", 1, mapCalls.get());
  }

  /**
   * 68-field JavaBean exercising chunked DirectSchemaCoder (>64 fields, 3 chunks) with nullable
   * getters, non-null constructor defaults, fluent setters, and single-evaluation getter tracking.
   */
  @DefaultSchema(JavaBeanSchema.class)
  public static class Wide68Bean {
    private final int[] iv = new int[34];
    private final @Nullable String[] sv = new String[34];
    final transient int[] strGetterCounts = new int[34];

    public Wide68Bean() {
      sv[1] = "default-s01";
      sv[33] = "default-s33";
    }

    private @Nullable String readStr(int idx) {
      strGetterCounts[idx]++;
      return sv[idx];
    }

    void resetGetterCounts() {
      Arrays.fill(strGetterCounts, 0);
    }

    static Wide68Bean sample(boolean sparseNulls) {
      Wide68Bean b = new Wide68Bean();
      for (int i = 0; i < 34; i++) {
        b.iv[i] = 100 + i;
        b.sv[i] = (sparseNulls && (i == 1 || i == 17 || i == 33)) ? null : ("str-" + i);
      }
      return b;
    }

    public int getI00() {
      return iv[0];
    }

    public void setI00(int v) {
      iv[0] = v;
    }

    public int getI01() {
      return iv[1];
    }

    public void setI01(int v) {
      iv[1] = v;
    }

    public int getI02() {
      return iv[2];
    }

    public void setI02(int v) {
      iv[2] = v;
    }

    public int getI03() {
      return iv[3];
    }

    public void setI03(int v) {
      iv[3] = v;
    }

    public int getI04() {
      return iv[4];
    }

    public void setI04(int v) {
      iv[4] = v;
    }

    public int getI05() {
      return iv[5];
    }

    public void setI05(int v) {
      iv[5] = v;
    }

    public int getI06() {
      return iv[6];
    }

    public void setI06(int v) {
      iv[6] = v;
    }

    public int getI07() {
      return iv[7];
    }

    public void setI07(int v) {
      iv[7] = v;
    }

    public int getI08() {
      return iv[8];
    }

    public void setI08(int v) {
      iv[8] = v;
    }

    public int getI09() {
      return iv[9];
    }

    public void setI09(int v) {
      iv[9] = v;
    }

    public int getI10() {
      return iv[10];
    }

    public void setI10(int v) {
      iv[10] = v;
    }

    public int getI11() {
      return iv[11];
    }

    public void setI11(int v) {
      iv[11] = v;
    }

    public int getI12() {
      return iv[12];
    }

    public void setI12(int v) {
      iv[12] = v;
    }

    public int getI13() {
      return iv[13];
    }

    public void setI13(int v) {
      iv[13] = v;
    }

    public int getI14() {
      return iv[14];
    }

    public void setI14(int v) {
      iv[14] = v;
    }

    public int getI15() {
      return iv[15];
    }

    public void setI15(int v) {
      iv[15] = v;
    }

    public int getI16() {
      return iv[16];
    }

    public void setI16(int v) {
      iv[16] = v;
    }

    public int getI17() {
      return iv[17];
    }

    public void setI17(int v) {
      iv[17] = v;
    }

    public int getI18() {
      return iv[18];
    }

    public void setI18(int v) {
      iv[18] = v;
    }

    public int getI19() {
      return iv[19];
    }

    public void setI19(int v) {
      iv[19] = v;
    }

    public int getI20() {
      return iv[20];
    }

    public void setI20(int v) {
      iv[20] = v;
    }

    public int getI21() {
      return iv[21];
    }

    public void setI21(int v) {
      iv[21] = v;
    }

    public int getI22() {
      return iv[22];
    }

    public void setI22(int v) {
      iv[22] = v;
    }

    public int getI23() {
      return iv[23];
    }

    public void setI23(int v) {
      iv[23] = v;
    }

    public int getI24() {
      return iv[24];
    }

    public void setI24(int v) {
      iv[24] = v;
    }

    public int getI25() {
      return iv[25];
    }

    public void setI25(int v) {
      iv[25] = v;
    }

    public int getI26() {
      return iv[26];
    }

    public void setI26(int v) {
      iv[26] = v;
    }

    public int getI27() {
      return iv[27];
    }

    public void setI27(int v) {
      iv[27] = v;
    }

    public int getI28() {
      return iv[28];
    }

    public void setI28(int v) {
      iv[28] = v;
    }

    public int getI29() {
      return iv[29];
    }

    public void setI29(int v) {
      iv[29] = v;
    }

    public int getI30() {
      return iv[30];
    }

    public void setI30(int v) {
      iv[30] = v;
    }

    public int getI31() {
      return iv[31];
    }

    public void setI31(int v) {
      iv[31] = v;
    }

    public int getI32() {
      return iv[32];
    }

    public void setI32(int v) {
      iv[32] = v;
    }

    public int getI33() {
      return iv[33];
    }

    public void setI33(int v) {
      iv[33] = v;
    }

    @SuppressWarnings("nullness")
    public String getS00() {
      return readStr(0);
    }

    public void setS00(String v) {
      sv[0] = v;
    }

    void clearRequiredS00ForTest() {
      sv[0] = null;
    }

    public @Nullable String getS01() {
      return readStr(1);
    }

    public Wide68Bean setS01(@Nullable String v) {
      sv[1] = v;
      return this;
    }

    public @Nullable String getS02() {
      return readStr(2);
    }

    public void setS02(@Nullable String v) {
      sv[2] = v;
    }

    public @Nullable String getS03() {
      return readStr(3);
    }

    public void setS03(@Nullable String v) {
      sv[3] = v;
    }

    public @Nullable String getS04() {
      return readStr(4);
    }

    public void setS04(@Nullable String v) {
      sv[4] = v;
    }

    public @Nullable String getS05() {
      return readStr(5);
    }

    public void setS05(@Nullable String v) {
      sv[5] = v;
    }

    public @Nullable String getS06() {
      return readStr(6);
    }

    public void setS06(@Nullable String v) {
      sv[6] = v;
    }

    public @Nullable String getS07() {
      return readStr(7);
    }

    public void setS07(@Nullable String v) {
      sv[7] = v;
    }

    public @Nullable String getS08() {
      return readStr(8);
    }

    public void setS08(@Nullable String v) {
      sv[8] = v;
    }

    public @Nullable String getS09() {
      return readStr(9);
    }

    public void setS09(@Nullable String v) {
      sv[9] = v;
    }

    public @Nullable String getS10() {
      return readStr(10);
    }

    public void setS10(@Nullable String v) {
      sv[10] = v;
    }

    public @Nullable String getS11() {
      return readStr(11);
    }

    public void setS11(@Nullable String v) {
      sv[11] = v;
    }

    public @Nullable String getS12() {
      return readStr(12);
    }

    public void setS12(@Nullable String v) {
      sv[12] = v;
    }

    public @Nullable String getS13() {
      return readStr(13);
    }

    public void setS13(@Nullable String v) {
      sv[13] = v;
    }

    public @Nullable String getS14() {
      return readStr(14);
    }

    public void setS14(@Nullable String v) {
      sv[14] = v;
    }

    public @Nullable String getS15() {
      return readStr(15);
    }

    public void setS15(@Nullable String v) {
      sv[15] = v;
    }

    public @Nullable String getS16() {
      return readStr(16);
    }

    public void setS16(@Nullable String v) {
      sv[16] = v;
    }

    public @Nullable String getS17() {
      return readStr(17);
    }

    public void setS17(@Nullable String v) {
      sv[17] = v;
    }

    public @Nullable String getS18() {
      return readStr(18);
    }

    public void setS18(@Nullable String v) {
      sv[18] = v;
    }

    public @Nullable String getS19() {
      return readStr(19);
    }

    public void setS19(@Nullable String v) {
      sv[19] = v;
    }

    public @Nullable String getS20() {
      return readStr(20);
    }

    public void setS20(@Nullable String v) {
      sv[20] = v;
    }

    public @Nullable String getS21() {
      return readStr(21);
    }

    public void setS21(@Nullable String v) {
      sv[21] = v;
    }

    public @Nullable String getS22() {
      return readStr(22);
    }

    public void setS22(@Nullable String v) {
      sv[22] = v;
    }

    public @Nullable String getS23() {
      return readStr(23);
    }

    public void setS23(@Nullable String v) {
      sv[23] = v;
    }

    public @Nullable String getS24() {
      return readStr(24);
    }

    public void setS24(@Nullable String v) {
      sv[24] = v;
    }

    public @Nullable String getS25() {
      return readStr(25);
    }

    public void setS25(@Nullable String v) {
      sv[25] = v;
    }

    public @Nullable String getS26() {
      return readStr(26);
    }

    public void setS26(@Nullable String v) {
      sv[26] = v;
    }

    public @Nullable String getS27() {
      return readStr(27);
    }

    public void setS27(@Nullable String v) {
      sv[27] = v;
    }

    public @Nullable String getS28() {
      return readStr(28);
    }

    public void setS28(@Nullable String v) {
      sv[28] = v;
    }

    public @Nullable String getS29() {
      return readStr(29);
    }

    public void setS29(@Nullable String v) {
      sv[29] = v;
    }

    public @Nullable String getS30() {
      return readStr(30);
    }

    public void setS30(@Nullable String v) {
      sv[30] = v;
    }

    public @Nullable String getS31() {
      return readStr(31);
    }

    public void setS31(@Nullable String v) {
      sv[31] = v;
    }

    public @Nullable String getS32() {
      return readStr(32);
    }

    public void setS32(@Nullable String v) {
      sv[32] = v;
    }

    public @Nullable String getS33() {
      return readStr(33);
    }

    public Wide68Bean setS33(@Nullable String v) {
      sv[33] = v;
      return this;
    }
  }

  @Test
  public void testChunkedDirectSchemaCoderWideJavaBeanEvaluateOnce() throws Exception {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SchemaCoder<Wide68Bean> coder = registry.getSchemaCoder(Wide68Bean.class);
    assertEquals(68, coder.getSchema().getFieldCount());
    assertTrue("Expected chunked DirectSchemaCoder for Wide68Bean", coder.hasDirectCoder());

    RowCoder rowCoder = RowCoder.of(coder.getSchema());
    for (boolean sparseNulls : new boolean[] {false, true}) {
      Wide68Bean sample = Wide68Bean.sample(sparseNulls);
      sample.resetGetterCounts();
      byte[] directBytes = encode(coder, sample);
      for (int i = 0; i < 34; i++) {
        assertEquals(
            "Nullable getter s" + i + " must be evaluated exactly once during chunked encode",
            1,
            sample.strGetterCounts[i]);
      }

      Row asRow = coder.getToRowFunction().apply(sample);
      byte[] rowBytes = encode(rowCoder, asRow);
      assertArrayEquals(rowBytes, directBytes);

      assertTrue(coder.isRegisterByteSizeObserverCheap(sample));
      assertEquals(directBytes.length, observeSize(coder, sample));

      Wide68Bean decoded = coder.decode(new ByteArrayInputStream(directBytes));
      assertEquals(asRow, coder.getToRowFunction().apply(decoded));
      if (sparseNulls) {
        assertNull(decoded.getS01());
        assertNull(decoded.getS17());
        assertNull(decoded.getS33());
      }
    }

    // Exercise chunk bit-31 null (s29 is field index 63 = chunk 1 bit 31 in alphabetical bean
    // order)
    // and Java serialization round-trip of chunked JavaBean SchemaCoder.
    SchemaCoder<Wide68Bean> cloned = SerializableUtils.clone(coder);
    assertTrue(cloned.hasDirectCoder());
    Wide68Bean bit31Null = Wide68Bean.sample(false);
    bit31Null.setS29(null);
    bit31Null.resetGetterCounts();
    byte[] bit31Bytes = encode(cloned, bit31Null);
    for (int i = 0; i < 34; i++) {
      assertEquals(1, bit31Null.strGetterCounts[i]);
    }
    Wide68Bean decodedBit31 = cloned.decode(new ByteArrayInputStream(bit31Bytes));
    assertNull(decodedBit31.getS29());
    assertEquals(
        cloned.getToRowFunction().apply(bit31Null), cloned.getToRowFunction().apply(decodedBit31));

    // Null in non-nullable reference field s00 must throw RuntimeException in nullScanBeanC* and
    // ByteSizeChunkImpl (emitThrowNullNonNullable).
    Wide68Bean missingReq = Wide68Bean.sample(false);
    missingReq.clearRequiredS00ForTest();
    RuntimeException encNullEx =
        assertThrows(RuntimeException.class, () -> encode(coder, missingReq));
    assertTrue(
        encNullEx.getMessage() != null
            && encNullEx.getMessage().contains("Null value set on non-nullable field"));
    RuntimeException sizeNullEx =
        assertThrows(RuntimeException.class, () -> observeSize(coder, missingReq));
    assertTrue(
        sizeNullEx.getMessage() != null
            && sizeNullEx.getMessage().contains("Null value set on non-nullable field"));
  }

  @Test
  public void testRowWireCodecVarIntAndVarLongEdgeCases() throws Exception {
    int[] intVals = {
      0,
      1,
      127,
      128,
      1000,
      16383,
      16384,
      (1 << 21) - 1,
      1 << 21,
      (1 << 28) - 1,
      1 << 28,
      Integer.MAX_VALUE,
      -1,
      -128,
      Integer.MIN_VALUE
    };
    for (int v : intVals) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      VarInt.encode(v, out);
      assertEquals(v, RowWireCodec.readVarInt(new ByteArrayInputStream(out.toByteArray())));
    }
    // Non-canonical 6-byte encoding of 0 must still decode to 0.
    assertEquals(
        0,
        RowWireCodec.readVarInt(
            new ByteArrayInputStream(
                new byte[] {(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0})));
    // 5-byte 32-bit overflow (1L << 32), 6-byte overflow (1L << 40), and negative long (-1L).
    for (long over : new long[] {1L << 32, 1L << 40, -1L}) {
      ByteArrayOutputStream overOut = new ByteArrayOutputStream();
      VarInt.encode(over, overOut);
      assertThrows(
          IOException.class,
          () -> RowWireCodec.readVarInt(new ByteArrayInputStream(overOut.toByteArray())));
    }
    // EOF on byte 0 vs unterminated at byte 2, 3, 4, 5, and 6.
    assertThrows(
        CoderException.class, () -> RowWireCodec.readVarInt(new ByteArrayInputStream(new byte[0])));
    for (int len = 1; len <= 5; len++) {
      byte[] trunc = new byte[len];
      Arrays.fill(trunc, (byte) 0x80);
      assertThrows(
          IOException.class, () -> RowWireCodec.readVarInt(new ByteArrayInputStream(trunc)));
    }

    long[] longVals = {
      0L,
      1L,
      127L,
      128L,
      16383L,
      16384L,
      (1L << 21) - 1,
      1L << 21,
      (1L << 28) - 1,
      1L << 28,
      1_000_000_000L,
      (1L << 35) - 1,
      1L << 35,
      1_700_000_000_000L,
      (1L << 49) - 1,
      (1L << 63) - 1,
      Long.MAX_VALUE,
      -1L,
      Long.MIN_VALUE
    };
    for (long v : longVals) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      VarInt.encode(v, out);
      assertEquals(v, RowWireCodec.readVarLong(new ByteArrayInputStream(out.toByteArray())));
    }
    assertThrows(
        CoderException.class,
        () -> RowWireCodec.readVarLong(new ByteArrayInputStream(new byte[0])));
    for (int len = 1; len <= 9; len++) {
      byte[] trunc = new byte[len];
      Arrays.fill(trunc, (byte) 0x81);
      assertThrows(
          IOException.class, () -> RowWireCodec.readVarLong(new ByteArrayInputStream(trunc)));
    }
    // 10th byte > 1 and 11-byte stream both overflow 64-bit unsigned varlong.
    byte[] bad10 = new byte[] {-1, -1, -1, -1, -1, -1, -1, -1, -1, 2};
    assertThrows(
        IOException.class, () -> RowWireCodec.readVarLong(new ByteArrayInputStream(bad10)));
    byte[] bad11 = new byte[] {-128, -128, -128, -128, -128, -128, -128, -128, -128, -128, 0};
    assertThrows(
        IOException.class, () -> RowWireCodec.readVarLong(new ByteArrayInputStream(bad11)));
  }

  /**
   * InputStream that delivers at most 1 byte per read(byte[], int, int) call to exercise
   * readExactSlow.
   */
  private static final class OneByteAtATimeInputStream extends InputStream {
    private final byte[] data;
    private int pos;

    OneByteAtATimeInputStream(byte[] data) {
      this.data = data;
    }

    @Override
    public int read() {
      return pos < data.length ? (data[pos++] & 0xFF) : -1;
    }

    @Override
    public int read(byte[] b, int off, int len) {
      if (pos >= data.length) {
        return -1;
      }
      if (len <= 0) {
        return 0;
      }
      b[off] = data[pos++];
      return 1;
    }
  }

  @Test
  public void testRowWireCodecZeroLenReadNullBytesShortVarHandleAndPools() throws Exception {
    byte[] scratch = new byte[512];
    // Zero-length reads at EOF on ByteArrayInputStream must succeed (C1 regression guard).
    assertEquals(0L, RowWireCodec.readNullMask(new ByteArrayInputStream(new byte[0]), 0, scratch));
    assertSame(
        scratch, RowWireCodec.readNullBytes(new ByteArrayInputStream(new byte[0]), 0, scratch));
    // Negative nullByteLen must throw CoderException in readNullBytes and packWideNullHeader.
    assertThrows(
        CoderException.class,
        () -> RowWireCodec.readNullBytes(new ByteArrayInputStream(new byte[4]), -1, scratch));
    assertThrows(
        CoderException.class,
        () ->
            RowWireCodec.packWideNullHeader(
                100, new byte[4], -1, scratch, new ByteArrayOutputStream()));

    // packWideNullHeader boundary tests: 0, 502 (fits 512), 503 & 513 (direct stream), small buf.
    for (int nullLen : new int[] {0, 502, 503, 513}) {
      byte[] bitmap = new byte[nullLen];
      for (int i = 0; i < nullLen; i++) {
        bitmap[i] = (byte) ((i * 31 + 7) & 0xFF);
      }
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      int pos = RowWireCodec.packWideNullHeader(5000, bitmap, nullLen, scratch, out);
      RowWireCodec.flush(scratch, pos, out);
      ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
      assertEquals(5000, RowWireCodec.readVarInt(in));
      int decodedLen = RowWireCodec.readVarInt(in);
      assertEquals(nullLen, decodedLen);
      byte[] read = RowWireCodec.readNullBytes(in, decodedLen, scratch);
      if (nullLen <= scratch.length) {
        assertSame(scratch, read);
      }
      for (int i = 0; i < nullLen; i++) {
        assertEquals(bitmap[i], read[i]);
      }
    }
    // Custom undersized buffer (< 10 bytes) must not throw ArrayIndexOutOfBoundsException (C3).
    ByteArrayOutputStream smallBufOut = new ByteArrayOutputStream();
    byte[] smallBuf = new byte[4];
    byte[] b8 = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
    int sp = RowWireCodec.packWideNullHeader(200, b8, 8, smallBuf, smallBufOut);
    RowWireCodec.flush(smallBuf, sp, smallBufOut);
    ByteArrayInputStream sbi = new ByteArrayInputStream(smallBufOut.toByteArray());
    assertEquals(200, RowWireCodec.readVarInt(sbi));
    assertEquals(8, RowWireCodec.readVarInt(sbi));
    byte[] readSmall = RowWireCodec.readNullBytes(sbi, 8, smallBuf);
    assertArrayEquals(b8, readSmall);

    // extractChunkNullMask32 partial chunks (1, 2, 3 bytes), full 4-byte chunk with bit 31 set,
    // OOB.
    assertEquals(0, RowWireCodec.extractChunkNullMask32(new byte[0], 0, 0));
    byte[] maskBytes = new byte[] {0x12, 0x34, 0x56, (byte) 0x80, 0x01, 0x02, 0x03};
    assertEquals(0x80563412, RowWireCodec.extractChunkNullMask32(maskBytes, 7, 0));
    assertEquals(0x00030201, RowWireCodec.extractChunkNullMask32(maskBytes, 7, 1));
    assertEquals(0x00000201, RowWireCodec.extractChunkNullMask32(maskBytes, 6, 1));
    assertEquals(0x00000001, RowWireCodec.extractChunkNullMask32(maskBytes, 5, 1));
    assertEquals(0, RowWireCodec.extractChunkNullMask32(maskBytes, 7, 2));

    // SHORT_BE VarHandle packInt16 / readInt16 boundaries vs BigEndianShortCoder.
    BigEndianShortCoder shortCoder = BigEndianShortCoder.of();
    for (short s :
        new short[] {Short.MIN_VALUE, -1000, -128, -1, 0, 1, 127, 1000, Short.MAX_VALUE}) {
      ByteArrayOutputStream actual = new ByteArrayOutputStream();
      int p = RowWireCodec.packInt16(s, scratch, 0, actual);
      RowWireCodec.flush(scratch, p, actual);
      byte[] expected = encode(shortCoder, s);
      assertArrayEquals(expected, actual.toByteArray());
      assertEquals(s, RowWireCodec.readInt16(new ByteArrayInputStream(expected), scratch));
      assertEquals(s, RowWireCodec.readInt16(new OneByteAtATimeInputStream(expected), scratch));
    }

    // Exercise readExactSlow partial reads on readDouble, readFloat, readString, readNullBytes.
    ByteArrayOutputStream slowStream = new ByteArrayOutputStream();
    int p = RowWireCodec.packDouble(3.141592653589793, scratch, 0, slowStream);
    p = RowWireCodec.packFloat(-2.5f, scratch, p, slowStream);
    p = RowWireCodec.packString("partial-read-utf8-café", scratch, p, slowStream);
    RowWireCodec.flush(scratch, p, slowStream);
    OneByteAtATimeInputStream slowIn = new OneByteAtATimeInputStream(slowStream.toByteArray());
    assertEquals(3.141592653589793, RowWireCodec.readDouble(slowIn, scratch), 0.0);
    assertEquals(-2.5f, RowWireCodec.readFloat(slowIn, scratch), 0.0f);
    assertEquals("partial-read-utf8-café", RowWireCodec.readString(slowIn, scratch));

    // Error branches on readByte, readBoolean, readString, readBytes.
    assertThrows(
        CoderException.class, () -> RowWireCodec.readByte(new ByteArrayInputStream(new byte[0])));
    assertThrows(
        CoderException.class,
        () -> RowWireCodec.readBoolean(new ByteArrayInputStream(new byte[0])));
    assertThrows(
        IOException.class,
        () -> RowWireCodec.readBoolean(new ByteArrayInputStream(new byte[] {2})));
    ByteArrayOutputStream negLen = new ByteArrayOutputStream();
    VarInt.encode(-1, negLen);
    assertThrows(
        CoderException.class,
        () -> RowWireCodec.readString(new ByteArrayInputStream(negLen.toByteArray()), scratch));
    assertThrows(
        CoderException.class,
        () -> RowWireCodec.readBytes(new ByteArrayInputStream(negLen.toByteArray())));
    assertArrayEquals(
        new byte[0], RowWireCodec.readBytes(new ByteArrayInputStream(new byte[] {0})));
    // >512-byte string hits readStringSlow.
    StringBuilder longSb = new StringBuilder(700);
    for (int i = 0; i < 700; i++) {
      longSb.append((char) ('a' + (i % 26)));
    }
    String longStr = longSb.toString();
    ByteArrayOutputStream longStrOut = new ByteArrayOutputStream();
    p = RowWireCodec.packString(longStr, scratch, 0, longStrOut);
    RowWireCodec.flush(scratch, p, longStrOut);
    assertEquals(
        longStr,
        RowWireCodec.readString(new ByteArrayInputStream(longStrOut.toByteArray()), scratch));

    // RefScratchPool re-entrancy & reference clearing for GC safety.
    RowWireCodec.RefScratchPool refPool = RowWireCodec.refScratchPool();
    Object[] r1 = refPool.acquire(10);
    Object[] r2 = refPool.acquire(10);
    assertTrue("Re-entrant acquire must return distinct arrays", r1 != r2);
    r1[0] = "secret";
    r1[9] = "tail";
    refPool.release(r1, 10);
    refPool.release(r2, 10);
    assertNull("Released RefScratchPool slots must be nulled for GC", r1[0]);
    assertNull("Released RefScratchPool slots must be nulled for GC", r1[9]);
  }

  @Test
  public void testWideRowCoderCutoffsOverridesRowBodyRichTypesEvolutionAndHuge4200()
      throws Exception {
    // Exact cutoff boundaries: 64 (unrolled), 65 (chunked), 1024 (chunked max), 1025 (loop).
    for (int width : new int[] {64, 65, 1024, 1025}) {
      Schema s = buildWideSchema(width, true, false);
      Coder<Row> c = RowCoderGenerator.generate(s);
      assertEquals(
          "hasGeneratedFieldCodec mismatch at width=" + width,
          width <= 1024,
          RowCoderGenerator.hasGeneratedFieldCodec(c));
      Row r = buildWideRow(s, true, true);
      byte[] bytes = encode(c, r);
      assertEquals(r, c.decode(new ByteArrayInputStream(bytes)));
      assertEquals(bytes.length, observeSize(c, r));
    }

    // Exercise encRowC* (non-RowWithStorage RowWithGetters on wide non-nullable + static schemas).
    for (boolean staticEnc : new boolean[] {false, true}) {
      Schema s70 = buildWideSchema(70, false, staticEnc);
      RowCoder c70 = RowCoder.of(s70);
      Row storageRow = buildWideRow(s70, false, true);
      List<FieldValueGetter<Row, Object>> getters = new ArrayList<>(70);
      for (int i = 0; i < 70; i++) {
        final int idx = i;
        getters.add(
            new FieldValueGetter<Row, Object>() {
              @Override
              public Object get(Row object) {
                return object.getValue(idx);
              }

              @Override
              public String name() {
                return s70.getField(idx).getName();
              }
            });
      }
      Row gettersRow =
          Row.withFieldValueGetters(s70, getters, storageRow, TypeDescriptor.of(Row.class));
      byte[] storageBytes = encode(c70, storageRow);
      byte[] gettersBytes = encode(c70, gettersRow);
      assertArrayEquals(storageBytes, gettersBytes);
      assertEquals(storageRow, c70.decode(new ByteArrayInputStream(gettersBytes)));
      assertTrue(c70.isRegisterByteSizeObserverCheap(gettersRow));
      assertEquals(gettersBytes.length, observeSize(c70, gettersRow));

      // Non-RandomAccess list on wide non-nullable / static schema.
      Object[] rawNn = new Object[70];
      for (int i = 0; i < 70; i++) {
        rawNn[i] = storageRow.getValue(i);
      }
      Row nonRaNn = Row.attachValues(s70, nonRandomAccessList(rawNn));
      assertArrayEquals(storageBytes, encode(c70, nonRaNn));
    }

    // Non-RandomAccess list and staticEncoding + nullable on wide schema.
    Schema s70Null = buildWideSchema(70, true, false);
    RowCoder c70Null = RowCoder.of(s70Null);
    Row arr70Null = buildWideRow(s70Null, true, true);
    Object[] raw70 = new Object[70];
    for (int i = 0; i < 70; i++) {
      raw70[i] = arr70Null.getValue(i);
    }
    Row nonRa70Null = Row.attachValues(s70Null, nonRandomAccessList(raw70));
    byte[] arr70Bytes = encode(c70Null, arr70Null);
    assertArrayEquals(arr70Bytes, encode(c70Null, nonRa70Null));
    assertEquals(arr70Null, c70Null.decode(new ByteArrayInputStream(arr70Bytes)));

    Schema s70StaticNull = buildWideSchema(70, true, true);
    RowCoder c70StaticNull = RowCoder.of(s70StaticNull);
    Row r70StaticNull = buildWideRow(s70StaticNull, false, true);
    byte[] staticNullBytes = encode(c70StaticNull, r70StaticNull);
    assertEquals(r70StaticNull, c70StaticNull.decode(new ByteArrayInputStream(staticNullBytes)));

    // Wide all-int schema exercises !needsDecodeScratch (0 ScratchPool checkouts) and O(1) isCheap.
    Schema.Builder intOnlyB = Schema.builder();
    Object[] intOnlyVals = new Object[70];
    for (int i = 0; i < 70; i++) {
      intOnlyB.addNullableInt32Field("i_" + i);
      intOnlyVals[i] = (i == 1 || i == 69) ? null : (i * 100);
    }
    Schema intOnlyS = intOnlyB.build();
    RowCoder intOnlyC = RowCoder.of(intOnlyS);
    Row intOnlyRow = Row.attachValues(intOnlyS, intOnlyVals);
    byte[] intOnlyBytes = encode(intOnlyC, intOnlyRow);
    assertEquals(intOnlyRow, intOnlyC.decode(new ByteArrayInputStream(intOnlyBytes)));
    assertTrue(intOnlyC.isRegisterByteSizeObserverCheap(intOnlyRow));
    assertEquals(intOnlyBytes.length, observeSize(intOnlyC, intOnlyRow));

    // Rich 70-field wide RowCoder touching coders, elemCoders, mid-chunk emitFlush, and all kinds.
    Schema nestedS = Schema.builder().addStringField("k").addInt32Field("v").build();
    Schema.Builder richB = Schema.builder();
    List<Object> richVals = new ArrayList<>(70);
    richB.addByteField("f_byte");
    richVals.add((byte) -7);
    richB.addInt16Field("f_short");
    richVals.add((short) -1234);
    richB.addDecimalField("f_dec");
    richVals.add(new BigDecimal("12345.67"));
    richB.addLogicalTypeField("f_date", new Date());
    richVals.add(LocalDate.of(2026, 5, 19));
    richB.addLogicalTypeField("f_time", new Time());
    richVals.add(LocalTime.of(12, 34, 56, 789));
    richB.addLogicalTypeField("f_ldt", new org.apache.beam.sdk.schemas.logicaltypes.DateTime());
    richVals.add(LocalDateTime.of(2026, 5, 19, 12, 34, 56));
    richB.addLogicalTypeField("f_uuid", new UuidLogicalType());
    richVals.add(new UUID(123L, 456L));
    richB.addLogicalTypeField("f_micros", new MicrosInstant());
    richVals.add(java.time.Instant.ofEpochSecond(1_700_000_000L, 123_000L));
    richB.addLogicalTypeField("f_nanos", new NanosInstant());
    richVals.add(java.time.Instant.ofEpochSecond(1_700_000_000L, 123_456_789L));
    richB.addLogicalTypeField("f_dur", new NanosDuration());
    richVals.add(Duration.ofSeconds(42, 999));
    richB.addArrayField("f_slist", FieldType.STRING);
    richVals.add(Arrays.asList("a", "b"));
    richB.addArrayField("f_ilist", FieldType.INT32);
    richVals.add(Arrays.asList(1, 2, 3));
    richB.addArrayField("f_llist", FieldType.INT64);
    richVals.add(Arrays.asList(10L, 20L));
    richB.addArrayField("f_dlist", FieldType.DOUBLE);
    richVals.add(Arrays.asList(1.5, 2.5));
    richB.addArrayField("f_rlist", FieldType.row(nestedS));
    richVals.add(
        Arrays.asList(
            Row.withSchema(nestedS).addValues("n1", 1).build(),
            Row.withSchema(nestedS).addValues("n2", 2).build()));
    richB.addMapField("f_slmap", FieldType.STRING, FieldType.INT64);
    richVals.add(ImmutableMap.of("x", 10L));
    richB.addMapField("f_ssmap", FieldType.STRING, FieldType.STRING);
    richVals.add(ImmutableMap.of("y", "z"));
    richB.addRowField("f_row", nestedS);
    richVals.add(Row.withSchema(nestedS).addValues("inner", 99).build());
    richB.addArrayField("f_other", FieldType.STRING.withNullable(true));
    richVals.add(Arrays.asList("ok", null, "tail"));
    for (int i = richVals.size(); i < 70; i++) {
      richB.addNullableStringField("pad_" + i);
      richVals.add((i % 11 == 0) ? null : ("p-" + i));
    }
    Schema richS = richB.build();
    RowCoder richC = RowCoder.of(richS);
    assertTrue(RowCoderGenerator.hasGeneratedFieldCodec(RowCoderGenerator.generate(richS)));
    Row richR = Row.withSchema(richS).attachValues(richVals);
    byte[] richBytes = encode(richC, richR);
    assertEquals(richR, richC.decode(new ByteArrayInputStream(richBytes)));
    assertEquals(richBytes.length, observeSize(richC, richR));

    // Non-identity encoding positions on wide 70-field chunked coder and 1050-field loop fallback.
    for (int width : new int[] {70, 1050}) {
      Schema s = buildWideSchema(width, true, false);
      Map<String, Integer> reversed = new HashMap<>();
      for (int i = 0; i < width; i++) {
        reversed.put("f_" + i, width - 1 - i);
      }
      RowCoderGenerator.overrideEncodingPositions(Objects.requireNonNull(s.getUUID()), reversed);
      RowCoder c = RowCoder.of(s);
      Row arrRow = buildWideRow(s, true, true);
      Row listRow = buildWideRow(s, true, false);
      byte[] bArr = encode(c, arrRow);
      byte[] bList = encode(c, listRow);
      assertArrayEquals(bArr, bList);
      assertEquals(arrRow, c.decode(new ByteArrayInputStream(bArr)));
      assertEquals(bArr.length, observeSize(c, arrRow));
    }

    // Shrinking evolution (300 -> 200 with nullByteLen == 0 and > 0) and early-null nullByteLen<=8.
    Schema s200 = buildWideSchema(200, true, false);
    Schema s300 = buildWideSchema(300, true, false);
    Row row300NoNull = buildWideRow(s300, false, true);
    byte[] b300NoNull = encode(RowCoder.of(s300), row300NoNull);
    Row dec200NoNull = RowCoder.of(s200).decode(new ByteArrayInputStream(b300NoNull));
    assertEquals(200, dec200NoNull.getFieldCount());
    assertEquals(row300NoNull.getInt32(0), dec200NoNull.getInt32(0));
    Row row300Sparse = buildWideRow(s300, true, true);
    byte[] b300Sparse = encode(RowCoder.of(s300), row300Sparse);
    Row dec200Sparse = RowCoder.of(s200).decode(new ByteArrayInputStream(b300Sparse));
    assertNull(dec200Sparse.getValue(0));
    assertEquals(row300Sparse.getInt64(1), dec200Sparse.getInt64(1));
    // Early-only null (field 1 null -> nullByteLen == 1 <= 8 on a 300-field chunked coder).
    Object[] earlyNullVals = new Object[300];
    for (int i = 0; i < 300; i++) {
      earlyNullVals[i] = row300NoNull.getValue(i);
    }
    earlyNullVals[1] = null;
    Row earlyNullRow = Row.attachValues(s300, earlyNullVals);
    byte[] earlyNullBytes = encode(RowCoder.of(s300), earlyNullRow);
    assertEquals(earlyNullRow, RowCoder.of(s300).decode(new ByteArrayInputStream(earlyNullBytes)));
    // Growing evolution with nullByteLen == 0 (200 -> 300).
    Row row200NoNull = buildWideRow(s200, false, true);
    byte[] b200NoNull = encode(RowCoder.of(s200), row200NoNull);
    Row dec300From200 = RowCoder.of(s300).decode(new ByteArrayInputStream(b200NoNull));
    assertEquals(row200NoNull.getInt32(0), dec300From200.getInt32(0));
    assertNull(dec300From200.getValue(250));

    // >4096 fields (4200 fields -> 525-byte null bitmap > SCRATCH_SIZE) with reversed positions.
    Schema.Builder hugeB = Schema.builder();
    Object[] hugeArr = new Object[4200];
    Map<String, Integer> hugePos = new HashMap<>(4200 * 2);
    for (int i = 0; i < 4200; i++) {
      hugeB.addNullableInt32Field("h_" + i);
      hugeArr[i] = (i == 0 || i == 2000 || i == 4199) ? null : i;
      hugePos.put("h_" + i, 4199 - i);
    }
    Schema hugeS = hugeB.build();
    RowCoderGenerator.overrideEncodingPositions(Objects.requireNonNull(hugeS.getUUID()), hugePos);
    RowCoder hugeC = RowCoder.of(hugeS);
    Row hugeArrRow = Row.attachValues(hugeS, hugeArr);
    Row hugeListRow = Row.withSchema(hugeS).attachValues(new ArrayList<>(Arrays.asList(hugeArr)));
    byte[] hugeBytesArr = encode(hugeC, hugeArrRow);
    byte[] hugeBytesList = encode(hugeC, hugeListRow);
    assertArrayEquals(hugeBytesArr, hugeBytesList);
    assertEquals(hugeArrRow, hugeC.decode(new ByteArrayInputStream(hugeBytesArr)));
    assertEquals(hugeBytesArr.length, observeSize(hugeC, hugeArrRow));
  }

  /** 68-field strictly non-nullable JavaBean exercising !nullableHeader && !isPojo chunked code. */
  @DefaultSchema(JavaBeanSchema.class)
  public static class Wide68NonNullBean {
    private int i00, i01, i02, i03, i04, i05, i06, i07, i08, i09;
    private int i10, i11, i12, i13, i14, i15, i16, i17, i18, i19;
    private int i20, i21, i22, i23, i24, i25, i26, i27, i28, i29;
    private int i30, i31, i32, i33;
    private String s00 = "", s01 = "", s02 = "", s03 = "", s04 = "", s05 = "", s06 = "", s07 = "";
    private String s08 = "", s09 = "", s10 = "", s11 = "", s12 = "", s13 = "", s14 = "", s15 = "";
    private String s16 = "", s17 = "", s18 = "", s19 = "", s20 = "", s21 = "", s22 = "", s23 = "";
    private String s24 = "", s25 = "", s26 = "", s27 = "", s28 = "", s29 = "", s30 = "", s31 = "";
    private String s32 = "", s33 = "";

    public Wide68NonNullBean() {}

    static Wide68NonNullBean sample() {
      Wide68NonNullBean b = new Wide68NonNullBean();
      b.i00 = 1;
      b.i33 = 34;
      b.s00 = "first";
      b.s33 = "last";
      return b;
    }

    public int getI00() {
      return i00;
    }

    public void setI00(int v) {
      i00 = v;
    }

    public int getI01() {
      return i01;
    }

    public void setI01(int v) {
      i01 = v;
    }

    public int getI02() {
      return i02;
    }

    public void setI02(int v) {
      i02 = v;
    }

    public int getI03() {
      return i03;
    }

    public void setI03(int v) {
      i03 = v;
    }

    public int getI04() {
      return i04;
    }

    public void setI04(int v) {
      i04 = v;
    }

    public int getI05() {
      return i05;
    }

    public void setI05(int v) {
      i05 = v;
    }

    public int getI06() {
      return i06;
    }

    public void setI06(int v) {
      i06 = v;
    }

    public int getI07() {
      return i07;
    }

    public void setI07(int v) {
      i07 = v;
    }

    public int getI08() {
      return i08;
    }

    public void setI08(int v) {
      i08 = v;
    }

    public int getI09() {
      return i09;
    }

    public void setI09(int v) {
      i09 = v;
    }

    public int getI10() {
      return i10;
    }

    public void setI10(int v) {
      i10 = v;
    }

    public int getI11() {
      return i11;
    }

    public void setI11(int v) {
      i11 = v;
    }

    public int getI12() {
      return i12;
    }

    public void setI12(int v) {
      i12 = v;
    }

    public int getI13() {
      return i13;
    }

    public void setI13(int v) {
      i13 = v;
    }

    public int getI14() {
      return i14;
    }

    public void setI14(int v) {
      i14 = v;
    }

    public int getI15() {
      return i15;
    }

    public void setI15(int v) {
      i15 = v;
    }

    public int getI16() {
      return i16;
    }

    public void setI16(int v) {
      i16 = v;
    }

    public int getI17() {
      return i17;
    }

    public void setI17(int v) {
      i17 = v;
    }

    public int getI18() {
      return i18;
    }

    public void setI18(int v) {
      i18 = v;
    }

    public int getI19() {
      return i19;
    }

    public void setI19(int v) {
      i19 = v;
    }

    public int getI20() {
      return i20;
    }

    public void setI20(int v) {
      i20 = v;
    }

    public int getI21() {
      return i21;
    }

    public void setI21(int v) {
      i21 = v;
    }

    public int getI22() {
      return i22;
    }

    public void setI22(int v) {
      i22 = v;
    }

    public int getI23() {
      return i23;
    }

    public void setI23(int v) {
      i23 = v;
    }

    public int getI24() {
      return i24;
    }

    public void setI24(int v) {
      i24 = v;
    }

    public int getI25() {
      return i25;
    }

    public void setI25(int v) {
      i25 = v;
    }

    public int getI26() {
      return i26;
    }

    public void setI26(int v) {
      i26 = v;
    }

    public int getI27() {
      return i27;
    }

    public void setI27(int v) {
      i27 = v;
    }

    public int getI28() {
      return i28;
    }

    public void setI28(int v) {
      i28 = v;
    }

    public int getI29() {
      return i29;
    }

    public void setI29(int v) {
      i29 = v;
    }

    public int getI30() {
      return i30;
    }

    public void setI30(int v) {
      i30 = v;
    }

    public int getI31() {
      return i31;
    }

    public void setI31(int v) {
      i31 = v;
    }

    public int getI32() {
      return i32;
    }

    public void setI32(int v) {
      i32 = v;
    }

    public int getI33() {
      return i33;
    }

    public void setI33(int v) {
      i33 = v;
    }

    public String getS00() {
      return s00;
    }

    public void setS00(String v) {
      s00 = v;
    }

    public String getS01() {
      return s01;
    }

    public void setS01(String v) {
      s01 = v;
    }

    public String getS02() {
      return s02;
    }

    public void setS02(String v) {
      s02 = v;
    }

    public String getS03() {
      return s03;
    }

    public void setS03(String v) {
      s03 = v;
    }

    public String getS04() {
      return s04;
    }

    public void setS04(String v) {
      s04 = v;
    }

    public String getS05() {
      return s05;
    }

    public void setS05(String v) {
      s05 = v;
    }

    public String getS06() {
      return s06;
    }

    public void setS06(String v) {
      s06 = v;
    }

    public String getS07() {
      return s07;
    }

    public void setS07(String v) {
      s07 = v;
    }

    public String getS08() {
      return s08;
    }

    public void setS08(String v) {
      s08 = v;
    }

    public String getS09() {
      return s09;
    }

    public void setS09(String v) {
      s09 = v;
    }

    public String getS10() {
      return s10;
    }

    public void setS10(String v) {
      s10 = v;
    }

    public String getS11() {
      return s11;
    }

    public void setS11(String v) {
      s11 = v;
    }

    public String getS12() {
      return s12;
    }

    public void setS12(String v) {
      s12 = v;
    }

    public String getS13() {
      return s13;
    }

    public void setS13(String v) {
      s13 = v;
    }

    public String getS14() {
      return s14;
    }

    public void setS14(String v) {
      s14 = v;
    }

    public String getS15() {
      return s15;
    }

    public void setS15(String v) {
      s15 = v;
    }

    public String getS16() {
      return s16;
    }

    public void setS16(String v) {
      s16 = v;
    }

    public String getS17() {
      return s17;
    }

    public void setS17(String v) {
      s17 = v;
    }

    public String getS18() {
      return s18;
    }

    public void setS18(String v) {
      s18 = v;
    }

    public String getS19() {
      return s19;
    }

    public void setS19(String v) {
      s19 = v;
    }

    public String getS20() {
      return s20;
    }

    public void setS20(String v) {
      s20 = v;
    }

    public String getS21() {
      return s21;
    }

    public void setS21(String v) {
      s21 = v;
    }

    public String getS22() {
      return s22;
    }

    public void setS22(String v) {
      s22 = v;
    }

    public String getS23() {
      return s23;
    }

    public void setS23(String v) {
      s23 = v;
    }

    public String getS24() {
      return s24;
    }

    public void setS24(String v) {
      s24 = v;
    }

    public String getS25() {
      return s25;
    }

    public void setS25(String v) {
      s25 = v;
    }

    public String getS26() {
      return s26;
    }

    public void setS26(String v) {
      s26 = v;
    }

    public String getS27() {
      return s27;
    }

    public void setS27(String v) {
      s27 = v;
    }

    public String getS28() {
      return s28;
    }

    public void setS28(String v) {
      s28 = v;
    }

    public String getS29() {
      return s29;
    }

    public void setS29(String v) {
      s29 = v;
    }

    public String getS30() {
      return s30;
    }

    public void setS30(String v) {
      s30 = v;
    }

    public String getS31() {
      return s31;
    }

    public void setS31(String v) {
      s31 = v;
    }

    public String getS32() {
      return s32;
    }

    public void setS32(String v) {
      s32 = v;
    }

    public String getS33() {
      return s33;
    }

    public void setS33(String v) {
      s33 = v;
    }
  }

  /**
   * 68-field strictly non-nullable POJO exercising !nullableHeader, staticEncoding, and IsCheap.
   */
  @DefaultSchema(JavaFieldSchema.class)
  public static class Wide68NonNullPojo {
    public byte fByte = 3;
    public short fShort = -12;
    public int fInt = 1000;
    public long fLong = 1_000_000_000L;
    public float fFloat = 2.5f;
    public double fDouble = 3.25;
    public boolean fBool = true;
    public String fStr = "hello";
    public byte[] fBytes = new byte[] {1, 2, 3};
    public BigDecimal fDec = new BigDecimal("12.34");
    public LocalDate fDate = LocalDate.of(2026, 1, 2);
    public Status fStatus = Status.ACTIVE;
    public java.util.Collection<String> fTags = Arrays.asList("t1", "t2");
    public java.util.Collection<NestedItem> fItems =
        Collections.singletonList(new NestedItem("item", 2, 4.5));
    public NestedItem fNested = new NestedItem("nested", 7, 8.5);
    public int p00, p01, p02, p03, p04, p05, p06, p07, p08, p09;
    public int p10, p11, p12, p13, p14, p15, p16, p17, p18, p19;
    public int p20, p21, p22, p23, p24, p25, p26, p27, p28, p29;
    public int p30, p31, p32, p33, p34, p35, p36, p37, p38, p39;
    public int p40, p41, p42, p43, p44, p45, p46, p47, p48, p49;
    public int p50, p51, p52;

    public Wide68NonNullPojo() {}
  }

  /** 68-field all-primitive POJO exercising !needsScratch decode and O(1) !anyNeedsIsCheap. */
  @DefaultSchema(JavaFieldSchema.class)
  public static class Wide68PrimitivesPojo {
    public int i00, i01, i02, i03, i04, i05, i06, i07, i08, i09;
    public int i10, i11, i12, i13, i14, i15, i16, i17, i18, i19;
    public int i20, i21, i22, i23, i24, i25, i26, i27, i28, i29;
    public int i30, i31, i32, i33;
    public long l00, l01, l02, l03, l04, l05, l06, l07, l08, l09;
    public long l10, l11, l12, l13, l14, l15, l16, l17, l18, l19;
    public long l20, l21, l22, l23, l24, l25, l26, l27, l28, l29;
    public long l30, l31, l32, l33;

    public Wide68PrimitivesPojo() {}
  }

  /**
   * 68-field nullable POJO with Map/String/Nested to test IsCheapChunkImpl Map & SUB_ROW branches.
   */
  @DefaultSchema(JavaFieldSchema.class)
  public static class Wide68MapNullablePojo {
    public @Nullable Map<String, Long> fMap;
    public @Nullable String fStr;
    public @Nullable NestedItem fNested;
    public @Nullable Integer fBoxInt;
    public int p00, p01, p02, p03, p04, p05, p06, p07, p08, p09;
    public int p10, p11, p12, p13, p14, p15, p16, p17, p18, p19;
    public int p20, p21, p22, p23, p24, p25, p26, p27, p28, p29;
    public int p30, p31, p32, p33, p34, p35, p36, p37, p38, p39;
    public int p40, p41, p42, p43, p44, p45, p46, p47, p48, p49;
    public int p50, p51, p52, p53, p54, p55, p56, p57, p58, p59;
    public int p60, p61, p62, p63;

    public Wide68MapNullablePojo() {}
  }

  /** 520-field POJO exceeding MAX_CHUNKED_DIRECT_FIELDS (512) to verify clean fallback. */
  @DefaultSchema(JavaFieldSchema.class)
  public static class Wide520Pojo {
    public int f000,
        f001,
        f002,
        f003,
        f004,
        f005,
        f006,
        f007,
        f008,
        f009,
        f010,
        f011,
        f012,
        f013,
        f014,
        f015,
        f016,
        f017,
        f018,
        f019;
    public int f020,
        f021,
        f022,
        f023,
        f024,
        f025,
        f026,
        f027,
        f028,
        f029,
        f030,
        f031,
        f032,
        f033,
        f034,
        f035,
        f036,
        f037,
        f038,
        f039;
    public int f040,
        f041,
        f042,
        f043,
        f044,
        f045,
        f046,
        f047,
        f048,
        f049,
        f050,
        f051,
        f052,
        f053,
        f054,
        f055,
        f056,
        f057,
        f058,
        f059;
    public int f060,
        f061,
        f062,
        f063,
        f064,
        f065,
        f066,
        f067,
        f068,
        f069,
        f070,
        f071,
        f072,
        f073,
        f074,
        f075,
        f076,
        f077,
        f078,
        f079;
    public int f080,
        f081,
        f082,
        f083,
        f084,
        f085,
        f086,
        f087,
        f088,
        f089,
        f090,
        f091,
        f092,
        f093,
        f094,
        f095,
        f096,
        f097,
        f098,
        f099;
    public int f100,
        f101,
        f102,
        f103,
        f104,
        f105,
        f106,
        f107,
        f108,
        f109,
        f110,
        f111,
        f112,
        f113,
        f114,
        f115,
        f116,
        f117,
        f118,
        f119;
    public int f120,
        f121,
        f122,
        f123,
        f124,
        f125,
        f126,
        f127,
        f128,
        f129,
        f130,
        f131,
        f132,
        f133,
        f134,
        f135,
        f136,
        f137,
        f138,
        f139;
    public int f140,
        f141,
        f142,
        f143,
        f144,
        f145,
        f146,
        f147,
        f148,
        f149,
        f150,
        f151,
        f152,
        f153,
        f154,
        f155,
        f156,
        f157,
        f158,
        f159;
    public int f160,
        f161,
        f162,
        f163,
        f164,
        f165,
        f166,
        f167,
        f168,
        f169,
        f170,
        f171,
        f172,
        f173,
        f174,
        f175,
        f176,
        f177,
        f178,
        f179;
    public int f180,
        f181,
        f182,
        f183,
        f184,
        f185,
        f186,
        f187,
        f188,
        f189,
        f190,
        f191,
        f192,
        f193,
        f194,
        f195,
        f196,
        f197,
        f198,
        f199;
    public int f200,
        f201,
        f202,
        f203,
        f204,
        f205,
        f206,
        f207,
        f208,
        f209,
        f210,
        f211,
        f212,
        f213,
        f214,
        f215,
        f216,
        f217,
        f218,
        f219;
    public int f220,
        f221,
        f222,
        f223,
        f224,
        f225,
        f226,
        f227,
        f228,
        f229,
        f230,
        f231,
        f232,
        f233,
        f234,
        f235,
        f236,
        f237,
        f238,
        f239;
    public int f240,
        f241,
        f242,
        f243,
        f244,
        f245,
        f246,
        f247,
        f248,
        f249,
        f250,
        f251,
        f252,
        f253,
        f254,
        f255,
        f256,
        f257,
        f258,
        f259;
    public int f260,
        f261,
        f262,
        f263,
        f264,
        f265,
        f266,
        f267,
        f268,
        f269,
        f270,
        f271,
        f272,
        f273,
        f274,
        f275,
        f276,
        f277,
        f278,
        f279;
    public int f280,
        f281,
        f282,
        f283,
        f284,
        f285,
        f286,
        f287,
        f288,
        f289,
        f290,
        f291,
        f292,
        f293,
        f294,
        f295,
        f296,
        f297,
        f298,
        f299;
    public int f300,
        f301,
        f302,
        f303,
        f304,
        f305,
        f306,
        f307,
        f308,
        f309,
        f310,
        f311,
        f312,
        f313,
        f314,
        f315,
        f316,
        f317,
        f318,
        f319;
    public int f320,
        f321,
        f322,
        f323,
        f324,
        f325,
        f326,
        f327,
        f328,
        f329,
        f330,
        f331,
        f332,
        f333,
        f334,
        f335,
        f336,
        f337,
        f338,
        f339;
    public int f340,
        f341,
        f342,
        f343,
        f344,
        f345,
        f346,
        f347,
        f348,
        f349,
        f350,
        f351,
        f352,
        f353,
        f354,
        f355,
        f356,
        f357,
        f358,
        f359;
    public int f360,
        f361,
        f362,
        f363,
        f364,
        f365,
        f366,
        f367,
        f368,
        f369,
        f370,
        f371,
        f372,
        f373,
        f374,
        f375,
        f376,
        f377,
        f378,
        f379;
    public int f380,
        f381,
        f382,
        f383,
        f384,
        f385,
        f386,
        f387,
        f388,
        f389,
        f390,
        f391,
        f392,
        f393,
        f394,
        f395,
        f396,
        f397,
        f398,
        f399;
    public int f400,
        f401,
        f402,
        f403,
        f404,
        f405,
        f406,
        f407,
        f408,
        f409,
        f410,
        f411,
        f412,
        f413,
        f414,
        f415,
        f416,
        f417,
        f418,
        f419;
    public int f420,
        f421,
        f422,
        f423,
        f424,
        f425,
        f426,
        f427,
        f428,
        f429,
        f430,
        f431,
        f432,
        f433,
        f434,
        f435,
        f436,
        f437,
        f438,
        f439;
    public int f440,
        f441,
        f442,
        f443,
        f444,
        f445,
        f446,
        f447,
        f448,
        f449,
        f450,
        f451,
        f452,
        f453,
        f454,
        f455,
        f456,
        f457,
        f458,
        f459;
    public int f460,
        f461,
        f462,
        f463,
        f464,
        f465,
        f466,
        f467,
        f468,
        f469,
        f470,
        f471,
        f472,
        f473,
        f474,
        f475,
        f476,
        f477,
        f478,
        f479;
    public int f480,
        f481,
        f482,
        f483,
        f484,
        f485,
        f486,
        f487,
        f488,
        f489,
        f490,
        f491,
        f492,
        f493,
        f494,
        f495,
        f496,
        f497,
        f498,
        f499;
    public int f500,
        f501,
        f502,
        f503,
        f504,
        f505,
        f506,
        f507,
        f508,
        f509,
        f510,
        f511,
        f512,
        f513,
        f514,
        f515,
        f516,
        f517,
        f518,
        f519;

    public Wide520Pojo() {}
  }

  @Test
  public void testChunkedDirectSchemaCoderNonNullableStaticRichEvolutionAndErrors()
      throws Exception {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SchemaCoder<Wide68NonNullPojo> coder = registry.getSchemaCoder(Wide68NonNullPojo.class);
    assertEquals(68, coder.getSchema().getFieldCount());
    assertFalse(coder.getSchema().hasNullableFields());
    assertTrue(coder.hasDirectCoder());

    Wide68NonNullPojo sample = new Wide68NonNullPojo();
    sample.p52 = 999;
    RowCoder rowCoder = RowCoder.of(coder.getSchema());
    byte[] directBytes = encode(coder, sample);
    Row asRow = coder.getToRowFunction().apply(sample);
    assertArrayEquals(encode(rowCoder, asRow), directBytes);
    assertEquals(directBytes.length, observeSize(coder, sample));
    assertEquals(
        asRow, coder.getToRowFunction().apply(coder.decode(new ByteArrayInputStream(directBytes))));

    // IsCheapChunkImpl: ArrayList -> false; CountingObservableCollection -> true & lazy;
    // long String (>1024) -> false; nested long String -> false; null -> false.
    assertFalse(coder.isRegisterByteSizeObserverCheap(null));
    assertFalse(coder.isRegisterByteSizeObserverCheap(sample));
    sample.fTags = new CountingObservableCollection<>(Arrays.asList("a", "b"));
    sample.fItems = new CountingObservableCollection<>(Collections.singletonList(sample.fNested));
    assertTrue(coder.isRegisterByteSizeObserverCheap(sample));
    ElementByteSizeObserver lazyObs =
        new ElementByteSizeObserver() {
          @Override
          protected void reportElementSize(long elementByteSize) {}
        };
    coder.registerByteSizeObserver(sample, lazyObs);
    assertTrue(lazyObs.getIsLazy());

    StringBuilder sb = new StringBuilder(1100);
    for (int i = 0; i < 1100; i++) {
      sb.append('x');
    }
    sample.fStr = sb.toString();
    assertFalse(coder.isRegisterByteSizeObserverCheap(sample));
    sample.fStr = "short";
    sample.fNested = new NestedItem(sb.toString(), 1, 1.0);
    assertFalse(coder.isRegisterByteSizeObserverCheap(sample));
    sample.fNested = new NestedItem("ok", 1, 1.0);

    // Null in non-nullable reference field throws RuntimeException in encode and byteSize.
    sample.fStr = null;
    assertThrows(RuntimeException.class, () -> encode(coder, sample));
    assertThrows(RuntimeException.class, () -> observeSize(coder, sample));
    sample.fStr = "restored";

    // Null top-level value throws CoderException in encode and registerByteSizeObserver.
    assertThrows(CoderException.class, () -> encode(coder, null));
    assertThrows(CoderException.class, () -> observeSize(coder, null));

    // Negative nullByteLen throws CoderException.
    ByteArrayOutputStream negNullStream = new ByteArrayOutputStream();
    VarInt.encode(68, negNullStream);
    VarInt.encode(-1, negNullStream);
    assertThrows(
        CoderException.class,
        () -> coder.decode(new ByteArrayInputStream(negNullStream.toByteArray())));

    // Non-nullable wide POJO decoding a stream with nullByteLen > 0 hits decodeEvolvedAfterNullLen.
    Wide68NonNullPojo freshSample = new Wide68NonNullPojo();
    freshSample.p52 = 999;
    byte[] freshBytes = encode(coder, freshSample);
    ByteArrayOutputStream withEmptyNonZeroBitmap = new ByteArrayOutputStream();
    VarInt.encode(68, withEmptyNonZeroBitmap);
    VarInt.encode(1, withEmptyNonZeroBitmap);
    withEmptyNonZeroBitmap.write(0); // 1-byte all-zero bitmap
    byte[] rawNoHeader = Arrays.copyOfRange(freshBytes, 2, freshBytes.length);
    withEmptyNonZeroBitmap.write(rawNoHeader);
    Wide68NonNullPojo evolvedNullBitmap =
        coder.decode(new ByteArrayInputStream(withEmptyNonZeroBitmap.toByteArray()));
    assertEquals(
        coder.getToRowFunction().apply(freshSample),
        coder.getToRowFunction().apply(evolvedNullBitmap));

    // Wide staticEncoding DirectSchemaCoder + encoding position override.
    JavaFieldSchema provider = new JavaFieldSchema();
    Schema staticSchema =
        Schema.builder()
            .addFields(coder.getSchema().getFields())
            .setOptions(
                Schema.Options.builder()
                    .setOption("beam:option:row:static_encoding", FieldType.BOOLEAN, true)
                    .build())
            .build();
    SchemaCoder<Wide68NonNullPojo> staticCoder =
        SchemaCoder.of(
            staticSchema,
            TypeDescriptor.of(Wide68NonNullPojo.class),
            provider
            .new ToRowWithValueGetters<>(staticSchema, TypeDescriptor.of(Wide68NonNullPojo.class)),
            provider.fromRowFunction(TypeDescriptor.of(Wide68NonNullPojo.class)));
    assertTrue(staticCoder.hasDirectCoder());
    byte[] staticDirect = encode(staticCoder, freshSample);
    byte[] staticRow =
        encode(RowCoder.of(staticSchema), staticCoder.getToRowFunction().apply(freshSample));
    assertArrayEquals(staticRow, staticDirect);
    assertEquals(staticDirect.length, observeSize(staticCoder, freshSample));
    assertEquals(
        staticCoder.getToRowFunction().apply(freshSample),
        staticCoder
            .getToRowFunction()
            .apply(staticCoder.decode(new ByteArrayInputStream(staticDirect))));

    Schema reorderedSchema = Schema.builder().addFields(coder.getSchema().getFields()).build();
    Map<String, Integer> revPos = new HashMap<>();
    for (int i = 0; i < 68; i++) {
      revPos.put(reorderedSchema.getField(i).getName(), 67 - i);
    }
    RowCoderGenerator.overrideEncodingPositions(
        Objects.requireNonNull(reorderedSchema.getUUID()), revPos);
    SchemaCoder<Wide68NonNullPojo> reorderedCoder =
        SchemaCoder.of(
            reorderedSchema,
            TypeDescriptor.of(Wide68NonNullPojo.class),
            provider
            .new ToRowWithValueGetters<>(
                reorderedSchema, TypeDescriptor.of(Wide68NonNullPojo.class)),
            provider.fromRowFunction(TypeDescriptor.of(Wide68NonNullPojo.class)));
    assertTrue(reorderedCoder.hasDirectCoder());
    byte[] reordDirect = encode(reorderedCoder, freshSample);
    assertArrayEquals(
        encode(RowCoder.of(reorderedSchema), reorderedCoder.getToRowFunction().apply(freshSample)),
        reordDirect);
    assertEquals(
        reorderedCoder.getToRowFunction().apply(freshSample),
        reorderedCoder
            .getToRowFunction()
            .apply(reorderedCoder.decode(new ByteArrayInputStream(reordDirect))));

    // All-primitive wide POJO (!needsScratch decode & O(1) !anyNeedsIsCheap).
    SchemaCoder<Wide68PrimitivesPojo> primCoder =
        registry.getSchemaCoder(Wide68PrimitivesPojo.class);
    assertTrue(primCoder.hasDirectCoder());
    Wide68PrimitivesPojo prim = new Wide68PrimitivesPojo();
    prim.i00 = 11;
    prim.i33 = -22;
    prim.l00 = 1_000_000_000L;
    prim.l33 = -99L;
    assertTrue(primCoder.isRegisterByteSizeObserverCheap(prim));
    assertFalse(primCoder.isRegisterByteSizeObserverCheap(null));
    byte[] primBytes = encode(primCoder, prim);
    assertEquals(primBytes.length, observeSize(primCoder, prim));
    assertEquals(
        primCoder.getToRowFunction().apply(prim),
        primCoder.getToRowFunction().apply(primCoder.decode(new ByteArrayInputStream(primBytes))));

    // Nullable Map + SUB_ROW IsCheapChunkImpl & evolution/primitive-null/truncated-stream errors.
    SchemaCoder<Wide68MapNullablePojo> mapCoder =
        registry.getSchemaCoder(Wide68MapNullablePojo.class);
    assertTrue(mapCoder.hasDirectCoder());
    Wide68MapNullablePojo mp = new Wide68MapNullablePojo();
    assertTrue(mapCoder.isRegisterByteSizeObserverCheap(mp));
    mp.fMap = ImmutableMap.of("k", 7L);
    assertFalse(mapCoder.isRegisterByteSizeObserverCheap(mp));
    byte[] mpBytes = encode(mapCoder, mp);
    assertEquals(mpBytes.length, observeSize(mapCoder, mp));
    assertEquals(
        mapCoder.getToRowFunction().apply(mp),
        mapCoder.getToRowFunction().apply(mapCoder.decode(new ByteArrayInputStream(mpBytes))));

    // Schema evolution on chunked DirectSchemaCoder: stream with 69 fields (extra trailing int).
    Schema s69 =
        Schema.builder()
            .addFields(mapCoder.getSchema().getFields())
            .addInt32Field("extra_69")
            .build();
    Object[] vals69 = new Object[69];
    Row mpRow = mapCoder.getToRowFunction().apply(mp);
    for (int i = 0; i < 68; i++) {
      vals69[i] = mpRow.getValue(i);
    }
    vals69[68] = 777;
    byte[] bytes69 = encode(RowCoder.of(s69), Row.attachValues(s69, vals69));
    Wide68MapNullablePojo decFrom69 = mapCoder.decode(new ByteArrayInputStream(bytes69));
    assertEquals(mpRow, mapCoder.getToRowFunction().apply(decFrom69));

    // Wide non-nullable JavaBean (!nullableHeader && !isPojo) normal + staticEncoding.
    SchemaCoder<Wide68NonNullBean> nnBeanCoder = registry.getSchemaCoder(Wide68NonNullBean.class);
    assertEquals(68, nnBeanCoder.getSchema().getFieldCount());
    assertFalse(nnBeanCoder.getSchema().hasNullableFields());
    assertTrue(nnBeanCoder.hasDirectCoder());
    Wide68NonNullBean nnBean = Wide68NonNullBean.sample();
    byte[] nnBeanBytes = encode(nnBeanCoder, nnBean);
    Row nnBeanRow = nnBeanCoder.getToRowFunction().apply(nnBean);
    assertArrayEquals(encode(RowCoder.of(nnBeanCoder.getSchema()), nnBeanRow), nnBeanBytes);
    assertEquals(nnBeanBytes.length, observeSize(nnBeanCoder, nnBean));
    assertEquals(
        nnBeanRow,
        nnBeanCoder
            .getToRowFunction()
            .apply(nnBeanCoder.decode(new ByteArrayInputStream(nnBeanBytes))));

    JavaBeanSchema beanProvider = new JavaBeanSchema();
    Schema staticBeanSchema =
        Schema.builder()
            .addFields(nnBeanCoder.getSchema().getFields())
            .setOptions(
                Schema.Options.builder()
                    .setOption("beam:option:row:static_encoding", FieldType.BOOLEAN, true)
                    .build())
            .build();
    SchemaCoder<Wide68NonNullBean> staticBeanCoder =
        SchemaCoder.of(
            staticBeanSchema,
            TypeDescriptor.of(Wide68NonNullBean.class),
            beanProvider
            .new ToRowWithValueGetters<>(
                staticBeanSchema, TypeDescriptor.of(Wide68NonNullBean.class)),
            beanProvider.fromRowFunction(TypeDescriptor.of(Wide68NonNullBean.class)));
    assertTrue(staticBeanCoder.hasDirectCoder());
    byte[] staticBeanBytes = encode(staticBeanCoder, nnBean);
    assertArrayEquals(
        encode(RowCoder.of(staticBeanSchema), staticBeanCoder.getToRowFunction().apply(nnBean)),
        staticBeanBytes);
    assertEquals(
        staticBeanCoder.getToRowFunction().apply(nnBean),
        staticBeanCoder
            .getToRowFunction()
            .apply(staticBeanCoder.decode(new ByteArrayInputStream(staticBeanBytes))));

    // Bit-31 null, streamFieldCount < count evolution (139 -> 140), primitive null -> NPE,
    // truncated stream -> CoderException, and clone on Wide140Pojo.
    SchemaCoder<Wide140Pojo> wide140Coder =
        SerializableUtils.clone(registry.getSchemaCoder(Wide140Pojo.class));
    assertTrue(wide140Coder.hasDirectCoder());
    Wide140Pojo bit31Pojo = Wide140Pojo.sample(false);
    bit31Pojo.s22 = null; // pos 127 = chunk 3 bit 31
    byte[] b140 = encode(wide140Coder, bit31Pojo);
    Wide140Pojo d140 = wide140Coder.decode(new ByteArrayInputStream(b140));
    assertNull(d140.s22);
    assertEquals(
        wide140Coder.getToRowFunction().apply(bit31Pojo),
        wide140Coder.getToRowFunction().apply(d140));
    // Growing evolution (139-field stream decoded by 140-field chunked DirectSchemaCoder).
    Schema s139 =
        Schema.builder().addFields(wide140Coder.getSchema().getFields().subList(0, 139)).build();
    Row row140 = wide140Coder.getToRowFunction().apply(bit31Pojo);
    Object[] vals139 = new Object[139];
    for (int i = 0; i < 139; i++) {
      vals139[i] = row140.getValue(i);
    }
    byte[] bytes139 = encode(RowCoder.of(s139), Row.attachValues(s139, vals139));
    Wide140Pojo decFrom139 = wide140Coder.decode(new ByteArrayInputStream(bytes139));
    assertNull(decFrom139.s34);
    assertEquals(bit31Pojo.i00, decFrom139.i00);
    assertEquals(bit31Pojo.s33, decFrom139.s33);
    // Truncated stream exercises catch0/catch1 scratch release.
    assertThrows(
        CoderException.class,
        () -> wide140Coder.decode(new ByteArrayInputStream(Arrays.copyOf(b140, 20))));
    // Null bit set on primitive field 0 (i00) throws NullPointerException (nullPrimitive).
    ByteArrayOutputStream primNullStream = new ByteArrayOutputStream();
    VarInt.encode(140, primNullStream);
    VarInt.encode(1, primNullStream);
    primNullStream.write(0x01); // field 0 (primitive int i00) marked null
    assertThrows(
        NullPointerException.class,
        () -> wide140Coder.decode(new ByteArrayInputStream(primNullStream.toByteArray())));

    // >512-field POJO falls back cleanly (!hasDirectCoder) and still round-trips.
    SchemaCoder<Wide520Pojo> wide520Coder = registry.getSchemaCoder(Wide520Pojo.class);
    assertEquals(520, wide520Coder.getSchema().getFieldCount());
    assertFalse(wide520Coder.hasDirectCoder());
    Wide520Pojo p520 = new Wide520Pojo();
    p520.f000 = 123;
    p520.f519 = 456;
    byte[] b520 = encode(wide520Coder, p520);
    Wide520Pojo d520 = wide520Coder.decode(new ByteArrayInputStream(b520));
    assertEquals(123, d520.f000);
    assertEquals(456, d520.f519);
  }

  /** >128-field POJO without a zero-arg constructor (regression test for T1/C2). */
  @DefaultSchema(JavaFieldSchema.class)
  public static class Wide130NoNoArgPojo {
    public int f000, f001, f002, f003, f004, f005, f006, f007, f008, f009;
    public int f010, f011, f012, f013, f014, f015, f016, f017, f018, f019;
    public int f020, f021, f022, f023, f024, f025, f026, f027, f028, f029;
    public int f030, f031, f032, f033, f034, f035, f036, f037, f038, f039;
    public int f040, f041, f042, f043, f044, f045, f046, f047, f048, f049;
    public int f050, f051, f052, f053, f054, f055, f056, f057, f058, f059;
    public int f060, f061, f062, f063, f064, f065, f066, f067, f068, f069;
    public int f070, f071, f072, f073, f074, f075, f076, f077, f078, f079;
    public int f080, f081, f082, f083, f084, f085, f086, f087, f088, f089;
    public int f090, f091, f092, f093, f094, f095, f096, f097, f098, f099;
    public int f100, f101, f102, f103, f104, f105, f106, f107, f108, f109;
    public int f110, f111, f112, f113, f114, f115, f116, f117, f118, f119;
    public int f120, f121, f122, f123, f124, f125, f126, f127, f128, f129;

    public Wide130NoNoArgPojo(int x) {
      this.f000 = x;
    }
  }

  public static class Wide130RichBase {
    public byte baseByte;
    public short baseShort;
    public float baseFloat;
    public boolean baseBool;
    public @Nullable Integer baseBoxInt;
  }

  /**
   * >128-field POJO subclass with inherited fields, all primitives, and non-trivial setter types.
   */
  @DefaultSchema(JavaFieldSchema.class)
  public static class Wide130RichSubPojo extends Wide130RichBase {
    public byte[] fBytes;
    public LocalDate fDate;
    public Status fStatus;
    public List<String> fList;
    public Map<String, Long> fMap;
    public NestedItem fNested;
    public @Nullable String fNullStr;
    public int p000, p001, p002, p003, p004, p005, p006, p007, p008, p009;
    public int p010, p011, p012, p013, p014, p015, p016, p017, p018, p019;
    public int p020, p021, p022, p023, p024, p025, p026, p027, p028, p029;
    public int p030, p031, p032, p033, p034, p035, p036, p037, p038, p039;
    public int p040, p041, p042, p043, p044, p045, p046, p047, p048, p049;
    public int p050, p051, p052, p053, p054, p055, p056, p057, p058, p059;
    public int p060, p061, p062, p063, p064, p065, p066, p067, p068, p069;
    public int p070, p071, p072, p073, p074, p075, p076, p077, p078, p079;
    public int p080, p081, p082, p083, p084, p085, p086, p087, p088, p089;
    public int p090, p091, p092, p093, p094, p095, p096, p097, p098, p099;
    public int p100, p101, p102, p103, p104, p105, p106, p107, p108, p109;
    public int p110, p111, p112, p113, p114, p115, p116, p117;

    public Wide130RichSubPojo() {}
  }

  @Test
  public void testChunkedPojoCreatorMissingNoArgAndRichSubclass130Fields() throws Exception {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    Schema noNoArgSchema = registry.getSchema(Wide130NoNoArgPojo.class);
    Object[] zeros = new Object[130];
    Arrays.fill(zeros, 0);
    Row noNoArgRow = Row.attachValues(noNoArgSchema, zeros);
    RuntimeException ex =
        assertThrows(
            RuntimeException.class,
            () -> registry.getFromRowFunction(Wide130NoNoArgPojo.class).apply(noNoArgRow));
    assertTrue(
        "Expected descriptive zero-arg constructor message, got: " + ex.getMessage(),
        ex.getMessage() != null && ex.getMessage().contains("zero-argument constructor"));

    SchemaCoder<Wide130RichSubPojo> coder = registry.getSchemaCoder(Wide130RichSubPojo.class);
    assertEquals(130, coder.getSchema().getFieldCount());
    Wide130RichSubPojo orig = new Wide130RichSubPojo();
    orig.baseByte = -9;
    orig.baseShort = 321;
    orig.baseFloat = 4.25f;
    orig.baseBool = true;
    orig.baseBoxInt = null;
    orig.fBytes = new byte[] {9, 8, 7};
    orig.fDate = LocalDate.of(2026, 7, 4);
    orig.fStatus = Status.ACTIVE;
    orig.fList = Arrays.asList("one", "two");
    orig.fMap = ImmutableMap.of("m", 42L);
    orig.fNested = new NestedItem("sub", 5, 6.5);
    orig.fNullStr = null;
    orig.p000 = 10;
    orig.p117 = 1170;

    Row row = coder.getToRowFunction().apply(orig);
    Wide130RichSubPojo recreated = coder.getFromRowFunction().apply(row);
    assertEquals(orig.baseByte, recreated.baseByte);
    assertEquals(orig.baseShort, recreated.baseShort);
    assertEquals(orig.baseFloat, recreated.baseFloat, 0.0f);
    assertEquals(orig.baseBool, recreated.baseBool);
    assertNull(recreated.baseBoxInt);
    assertArrayEquals(orig.fBytes, recreated.fBytes);
    assertEquals(orig.fDate, recreated.fDate);
    assertEquals(orig.fStatus, recreated.fStatus);
    assertEquals(orig.fList, recreated.fList);
    assertEquals(orig.fMap, recreated.fMap);
    assertEquals(orig.fNested, recreated.fNested);
    assertNull(recreated.fNullStr);
    assertEquals(10, recreated.p000);
    assertEquals(1170, recreated.p117);
  }

  @Test
  public void testSchemaCompactContainerCacheZeroBit63IterableAndSerialization() {
    // 0 container fields: count == 0, valid index -> -1, OOB throws IndexOutOfBoundsException.
    Schema noContainers = Schema.builder().addInt32Field("a").addStringField("b").build();
    assertEquals(0, noContainers.getCacheableContainerCount());
    assertEquals(-1, noContainers.getContainerCacheSlot(0));
    assertEquals(-1, noContainers.getContainerCacheSlot(1));
    assertThrows(IndexOutOfBoundsException.class, () -> noContainers.getContainerCacheSlot(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> noContainers.getContainerCacheSlot(2));

    // Wide 300-field schema with containers at index 0, 5, 63 (bit 63 POPCNT!), 64, and 250.
    Schema.Builder b = Schema.builder();
    for (int i = 0; i < 300; i++) {
      if (i == 0) {
        b.addArrayField("c_0", FieldType.STRING);
      } else if (i == 5) {
        b.addIterableField("c_5", FieldType.INT32);
      } else if (i == 63) {
        b.addMapField("c_63", FieldType.STRING, FieldType.INT64);
      } else if (i == 64) {
        b.addArrayField("c_64", FieldType.STRING);
      } else if (i == 250) {
        b.addIterableField("c_250", FieldType.STRING);
      } else {
        b.addInt32Field("f_" + i);
      }
    }
    Schema original = b.build();
    for (Schema s : new Schema[] {original, SerializableUtils.clone(original)}) {
      assertEquals(5, s.getCacheableContainerCount());
      assertEquals(0, s.getContainerCacheSlot(0));
      assertEquals(1, s.getContainerCacheSlot(5));
      assertEquals(2, s.getContainerCacheSlot(63));
      assertEquals(3, s.getContainerCacheSlot(64));
      assertEquals(4, s.getContainerCacheSlot(250));
      assertEquals(-1, s.getContainerCacheSlot(1));
      assertEquals(-1, s.getContainerCacheSlot(62));
      assertEquals(-1, s.getContainerCacheSlot(65));
      assertEquals(-1, s.getContainerCacheSlot(299));
      assertThrows(IndexOutOfBoundsException.class, () -> s.getContainerCacheSlot(-1));
      assertThrows(IndexOutOfBoundsException.class, () -> s.getContainerCacheSlot(300));
    }

    // Verify RowWithGetters caches ITERABLE at index 250.
    AtomicInteger iterCalls = new AtomicInteger();
    List<FieldValueGetter<Object, Object>> getters = new ArrayList<>(300);
    for (int i = 0; i < 300; i++) {
      final int idx = i;
      getters.add(
          new FieldValueGetter<Object, Object>() {
            @Override
            public Object get(Object object) {
              if (idx == 250) {
                iterCalls.incrementAndGet();
                return Arrays.asList("it1", "it2");
              }
              return idx;
            }

            @Override
            public String name() {
              return original.getField(idx).getName();
            }
          });
    }
    Row row =
        Row.withFieldValueGetters(original, getters, new Object(), TypeDescriptor.of(Object.class));
    List<String> first = new ArrayList<>();
    Objects.requireNonNull(row.getIterable(250)).forEach(e -> first.add((String) e));
    List<String> second = new ArrayList<>();
    Objects.requireNonNull(row.getIterable(250)).forEach(e -> second.add((String) e));
    assertEquals(Arrays.asList("it1", "it2"), first);
    assertEquals(Arrays.asList("it1", "it2"), second);
    assertEquals(1, iterCalls.get());
  }
}
