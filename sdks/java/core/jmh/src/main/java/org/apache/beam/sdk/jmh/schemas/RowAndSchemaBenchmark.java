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

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.jmh.schemas.SchemaCoderBenchmark.Color;
import org.apache.beam.sdk.jmh.schemas.SchemaCoderBenchmark.FlatJavaBean;
import org.apache.beam.sdk.jmh.schemas.SchemaCoderBenchmark.FlatPojo;
import org.apache.beam.sdk.jmh.schemas.SchemaCoderBenchmark.NestedPojo;
import org.apache.beam.sdk.jmh.schemas.SchemaCoderBenchmark.RichPojo;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.RowMessages;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.logicaltypes.DateTime;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.schemas.logicaltypes.FixedPrecisionNumeric;
import org.apache.beam.sdk.schemas.logicaltypes.OneOfType;
import org.apache.beam.sdk.schemas.logicaltypes.Timestamp;
import org.apache.beam.sdk.schemas.logicaltypes.UuidLogicalType;
import org.apache.beam.sdk.schemas.utils.JsonUtils;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Instant;
import org.joda.time.ReadableDateTime;
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
import org.openjdk.jmh.infra.Blackhole;

/**
 * JMH benchmarks for {@link Row} ({@code RowWithStorage} and {@code RowWithGetters}), {@link
 * Schema}, {@link Schema.LogicalType}, {@link RowMessages}, {@link JsonUtils}, and {@code
 * GetterBasedSchemaProvider} / ByteBuddy conversions.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@SuppressWarnings({"nullness", "rawtypes"})
public class RowAndSchemaBenchmark {

  @State(Scope.Thread)
  public static class RowState {
    Schema schemaA;
    Schema schemaB; // Equal fields, different random UUID
    Row rowStorageA;
    Row rowStorageB;
    Row rowWithGettersRich;
    int fieldIdxTags;
    int fieldIdxCounts;
    int fieldIdxAddress;

    SerializableFunction<FlatPojo, Row> flatPojoToRow;
    SerializableFunction<Row, FlatPojo> flatPojoFromRow;
    FlatPojo flatPojo;
    Row flatPojoRowStorage;

    SerializableFunction<RichPojo, Row> richPojoToRow;
    SerializableFunction<Row, RichPojo> richPojoFromRow;
    RichPojo richPojo;
    Row richPojoRowStorage;

    SerializableFunction<FlatJavaBean, Row> beanToRow;
    SerializableFunction<Row, FlatJavaBean> beanFromRow;
    FlatJavaBean bean;
    Row beanRowStorage;

    SimpleFunction<Row, byte[]> rowToBytes;
    Schema jsonSchema;
    Row jsonRow;
    SerializableFunction<Row, byte[]> rowToJsonBytes;
    SerializableFunction<byte[], Row> jsonBytesToRow;
    byte[] jsonBytes;

    Timestamp tsMicros;
    java.time.Instant javaInstant;
    Row tsBaseRow;

    DateTime dtLogical;
    LocalDateTime ldt;
    Row dtBaseRow;

    UuidLogicalType uuidLogical;
    UUID uuid;
    Row uuidBaseRow;

    EnumerationType enumLogical;
    OneOfType oneOfLogical;
    OneOfType.Value oneOfVal;
    Row oneOfBaseRow;

    FixedPrecisionNumeric fixedNumeric;
    BigDecimal numericVal;

    // Non-constant benchmark inputs to prevent JIT constant folding.
    String enumLookupName;
    int schemaFieldIdx;
    Schema.TypeName[] typeNames;
    int typeNameCursor;

    @Setup
    public void setUp() throws Exception {
      schemaA = buildTestSchema();
      schemaB = buildTestSchema();

      Instant tsA = Instant.ofEpochMilli(1_700_000_000_000L);
      Instant tsB = Instant.ofEpochMilli(1_700_000_000_000L);
      rowStorageA =
          Row.withSchema(schemaA)
              .addValues(
                  100_000L,
                  String.valueOf("user-42".toCharArray()),
                  100_000,
                  99.95,
                  true,
                  tsA,
                  new byte[] {1, 2, 3, 4})
              .build();
      rowStorageB =
          Row.withSchema(schemaB)
              .addValues(
                  Long.valueOf(100_000L),
                  String.valueOf("user-42".toCharArray()),
                  Integer.valueOf(100_000),
                  Double.valueOf(99.95),
                  true,
                  tsB,
                  new byte[] {1, 2, 3, 4})
              .build();
      Instant ts = tsA;

      SchemaRegistry registry = SchemaRegistry.createDefault();
      SchemaCoder<FlatPojo> flatCoder = registry.getSchemaCoder(FlatPojo.class);
      flatPojoToRow = flatCoder.getToRowFunction();
      flatPojoFromRow = flatCoder.getFromRowFunction();
      flatPojo = new FlatPojo();
      flatPojo.fByte = 7;
      flatPojo.fShort = 42;
      flatPojo.fInt = 123;
      flatPojo.fLong = 456L;
      flatPojo.fFloat = 1.5f;
      flatPojo.fDouble = 2.5;
      flatPojo.fBool = true;
      flatPojo.fString1 = "alpha";
      flatPojo.fString2 = "beta";
      flatPojo.fInstant = ts;
      flatPojoRowStorage = copyToStorage(flatCoder.getSchema(), flatPojoToRow.apply(flatPojo));

      SchemaCoder<RichPojo> richCoder = registry.getSchemaCoder(RichPojo.class);
      richPojoToRow = richCoder.getToRowFunction();
      richPojoFromRow = richCoder.getFromRowFunction();
      Map<String, Long> counts = new HashMap<>();
      counts.put("k1", 1L);
      counts.put("k2", 2L);
      counts.put("k3", 3L);
      counts.put("k4", 4L);
      richPojo = new RichPojo();
      richPojo.id = 42L;
      richPojo.name = "widget";
      richPojo.score = 98.5;
      richPojo.active = true;
      richPojo.ts = ts;
      richPojo.color = Color.BLUE;
      richPojo.uuid = new UUID(0x123456789ABCDEF0L, 0x0FEDCBA987654321L);
      richPojo.createdAt = LocalDateTime.of(2026, 5, 15, 14, 30, 0);
      richPojo.tags = Arrays.asList("a", "b", "c", "d");
      richPojo.counts = counts;
      richPojo.address = new NestedPojo("Seattle", 98101, 47.6062);
      rowWithGettersRich = richPojoToRow.apply(richPojo);
      Schema richSchema = richCoder.getSchema();
      fieldIdxTags = richSchema.indexOf("tags");
      fieldIdxCounts = richSchema.indexOf("counts");
      fieldIdxAddress = richSchema.indexOf("address");
      richPojoRowStorage = copyToStorage(richSchema, rowWithGettersRich);

      SchemaCoder<FlatJavaBean> beanCoder = registry.getSchemaCoder(FlatJavaBean.class);
      beanToRow = beanCoder.getToRowFunction();
      beanFromRow = beanCoder.getFromRowFunction();
      bean = new FlatJavaBean();
      bean.setFInt(123);
      bean.setFLong(456L);
      bean.setFDouble(2.5);
      bean.setFBool(true);
      bean.setFString1("alpha");
      bean.setFString2("beta");
      bean.setFInstant(ts);
      bean.setTags(Arrays.asList("a", "b", "c", "d"));
      bean.setCounts(counts);
      beanRowStorage = copyToStorage(beanCoder.getSchema(), beanToRow.apply(bean));

      JavaFieldSchema provider = new JavaFieldSchema();
      rowToBytes = RowMessages.rowToBytesFn(provider, TypeDescriptor.of(FlatPojo.class), flatCoder);

      jsonSchema =
          Schema.builder()
              .addInt64Field("id")
              .addStringField("user")
              .addInt32Field("count")
              .addDoubleField("amount")
              .addBooleanField("active")
              .addDateTimeField("ts")
              .build();
      jsonRow = Row.withSchema(jsonSchema).addValues(1L, "user-42", 100, 99.95, true, ts).build();
      rowToJsonBytes = JsonUtils.getRowToJsonBytesFunction(jsonSchema);
      jsonBytesToRow = JsonUtils.getJsonBytesToRowFunction(jsonSchema);
      jsonBytes = rowToJsonBytes.apply(jsonRow);

      tsMicros = Timestamp.MICROS;
      javaInstant = java.time.Instant.ofEpochSecond(1_700_000_000L, 123_456_000L);
      tsBaseRow = tsMicros.toBaseType(javaInstant);

      dtLogical = new DateTime();
      ldt = LocalDateTime.of(2026, 5, 15, 14, 30, 45);
      dtBaseRow = dtLogical.toBaseType(ldt);

      uuidLogical = new UuidLogicalType();
      uuid = new UUID(0x123456789ABCDEF0L, 0x0FEDCBA987654321L);
      uuidBaseRow = uuidLogical.toBaseType(uuid);

      enumLogical = EnumerationType.create("RED", "GREEN", "BLUE", "YELLOW");
      oneOfLogical =
          OneOfType.create(
              Schema.Field.of("a", FieldType.STRING),
              Schema.Field.of("b", FieldType.INT64),
              Schema.Field.of("c", FieldType.DOUBLE),
              Schema.Field.of("d", FieldType.BOOLEAN));
      oneOfVal = oneOfLogical.createValue("b", 999L);
      oneOfBaseRow = oneOfLogical.toBaseType(oneOfVal);

      fixedNumeric = FixedPrecisionNumeric.of(10, 2);
      numericVal = new BigDecimal("1234.56");

      enumLookupName = String.valueOf("BLUE".toCharArray());
      schemaFieldIdx = 3;
      typeNames = Schema.TypeName.values();
      typeNameCursor = 0;
    }

    private static Schema buildTestSchema() {
      return Schema.builder()
          .addInt64Field("id")
          .addStringField("user")
          .addInt32Field("count")
          .addDoubleField("amount")
          .addBooleanField("active")
          .addDateTimeField("ts")
          .addByteArrayField("payload")
          .build();
    }

    private static Row copyToStorage(Schema schema, Row row) throws Exception {
      RowCoder coder = RowCoder.of(schema);
      java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
      coder.encode(row, baos);
      return coder.decode(new java.io.ByteArrayInputStream(baos.toByteArray()));
    }
  }

  @Benchmark
  public boolean rowEqualsDifferentSchemaUuid(RowState s) {
    return s.rowStorageA.equals(s.rowStorageB);
  }

  @Benchmark
  public int rowHashCode(RowState s) {
    return s.rowStorageA.hashCode();
  }

  @Benchmark
  public ReadableDateTime rowGetDateTime(RowState s) {
    return s.rowStorageA.getDateTime(5);
  }

  @Benchmark
  public List<Object> rowGetBaseValues(RowState s) {
    return s.rowStorageA.getBaseValues();
  }

  @Benchmark
  public void rowWithGettersCachedCollectionRead(RowState s, Blackhole bh) {
    // Hits RowWithGetters cache for ARRAY and MAP fields
    bh.consume(s.rowWithGettersRich.getValue(s.fieldIdxTags));
    bh.consume(s.rowWithGettersRich.getValue(s.fieldIdxCounts));
  }

  @Benchmark
  public void rowWithGettersFreshReadAll(RowState s, Blackhole bh) {
    Row r = s.richPojoToRow.apply(s.richPojo);
    int n = r.getFieldCount();
    for (int i = 0; i < n; i++) {
      bh.consume(r.getValue(i));
    }
  }

  @Benchmark
  public Object rowWithGettersNestedRowRead(RowState s) {
    Row sub = s.rowWithGettersRich.getRow(s.fieldIdxAddress);
    return sub.getValue(0);
  }

  @Benchmark
  public FlatPojo flatPojoFromRow(RowState s) {
    return s.flatPojoFromRow.apply(s.flatPojoRowStorage);
  }

  @Benchmark
  public RichPojo richPojoFromRow(RowState s) {
    return s.richPojoFromRow.apply(s.richPojoRowStorage);
  }

  @Benchmark
  public FlatJavaBean javaBeanFromRow(RowState s) {
    return s.beanFromRow.apply(s.beanRowStorage);
  }

  @Benchmark
  public boolean schemaEqualsDifferentUuid(RowState s) {
    return s.schemaA.equals(s.schemaB);
  }

  @Benchmark
  public boolean schemaEquivalent(RowState s) {
    return s.schemaA.equivalent(s.schemaB);
  }

  @Benchmark
  public String schemaNameOf(RowState s) {
    return s.schemaA.nameOf(s.schemaFieldIdx);
  }

  @Benchmark
  public List<String> schemaGetFieldNames(RowState s) {
    return s.schemaA.getFieldNames();
  }

  @Benchmark
  public boolean typeNamePredicates(RowState s) {
    int idx = s.typeNameCursor + 1;
    if (idx >= s.typeNames.length) {
      idx = 0;
    }
    s.typeNameCursor = idx;
    Schema.TypeName t = s.typeNames[idx];
    return t.isPrimitiveType() && t.isNumericType() && !t.isCollectionType();
  }

  @Benchmark
  public byte[] rowMessagesToBytes(RowState s) {
    return s.rowToBytes.apply(s.flatPojoRowStorage);
  }

  @Benchmark
  public byte[] jsonRowToBytes(RowState s) {
    return s.rowToJsonBytes.apply(s.jsonRow);
  }

  @Benchmark
  public Row jsonBytesToRow(RowState s) {
    return s.jsonBytesToRow.apply(s.jsonBytes);
  }

  @Benchmark
  public Row logicalTimestampToBase(RowState s) {
    return s.tsMicros.toBaseType(s.javaInstant);
  }

  @Benchmark
  public java.time.Instant logicalTimestampToInput(RowState s) {
    return s.tsMicros.toInputType(s.tsBaseRow);
  }

  @Benchmark
  public Row logicalDateTimeToBase(RowState s) {
    return s.dtLogical.toBaseType(s.ldt);
  }

  @Benchmark
  public LocalDateTime logicalDateTimeToInput(RowState s) {
    return s.dtLogical.toInputType(s.dtBaseRow);
  }

  @Benchmark
  public Row logicalUuidToBase(RowState s) {
    return s.uuidLogical.toBaseType(s.uuid);
  }

  @Benchmark
  public UUID logicalUuidToInput(RowState s) {
    return s.uuidLogical.toInputType(s.uuidBaseRow);
  }

  @Benchmark
  public EnumerationType.Value logicalEnumValueOf(RowState s) {
    return s.enumLogical.valueOf(s.enumLookupName);
  }

  @Benchmark
  public Row logicalOneOfToBase(RowState s) {
    return s.oneOfLogical.toBaseType(s.oneOfVal);
  }

  @Benchmark
  public OneOfType.Value logicalOneOfToInput(RowState s) {
    return s.oneOfLogical.toInputType(s.oneOfBaseRow);
  }

  @Benchmark
  public BigDecimal logicalFixedNumericToInput(RowState s) {
    return s.fixedNumeric.toInputType(s.numericVal);
  }
}
