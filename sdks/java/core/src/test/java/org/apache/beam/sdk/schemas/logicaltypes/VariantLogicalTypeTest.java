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
package org.apache.beam.sdk.schemas.logicaltypes;

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.sdk.util.RowJsonUtils.jsonToRow;
import static org.apache.beam.sdk.util.RowJsonUtils.newObjectMapperWith;
import static org.apache.beam.sdk.util.RowJsonUtils.rowToJson;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.schemas.variant.Variant;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ToJson;
import org.apache.beam.sdk.util.RowJson.RowJsonDeserializer;
import org.apache.beam.sdk.util.RowJson.RowJsonSerializer;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link VariantLogicalType} and its integration with Beam schemas. */
@RunWith(JUnit4.class)
public class VariantLogicalTypeTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static final Schema EVENT_SCHEMA =
      Schema.builder().addInt64Field("id").addLogicalTypeField("payload", SqlTypes.VARIANT).build();

  private static final Variant PAYLOAD =
      Variant.parseJson("{\"event\":\"login\",\"device\":{\"os\":\"ios\"}}");

  @Test
  public void testIdentifierAndRepresentation() {
    assertEquals("beam:logical_type:variant:v1", VariantLogicalType.IDENTIFIER);
    assertEquals(VariantLogicalType.IDENTIFIER, SqlTypes.VARIANT.getIdentifier());
    assertEquals(
        FieldType.row(
            Schema.builder().addByteArrayField("metadata").addByteArrayField("value").build()),
        SqlTypes.VARIANT.getBaseType());

    Row base = SqlTypes.VARIANT.toBaseType(PAYLOAD);
    assertArrayEquals(PAYLOAD.metadata(), base.getBytes("metadata"));
    assertArrayEquals(PAYLOAD.value(), base.getBytes("value"));
    assertEquals(PAYLOAD, SqlTypes.VARIANT.toInputType(base));
  }

  @Test
  public void testRowAccess() {
    Row row = Row.withSchema(EVENT_SCHEMA).addValues(1L, PAYLOAD).build();
    Variant payload = row.getLogicalTypeValue("payload", Variant.class);
    assertEquals("ios", payload.get("$.device.os").getString());
    assertEquals("{\"device\":{\"os\":\"ios\"},\"event\":\"login\"}", payload.toJson());
  }

  @Test
  public void testRowCoderRoundTrip() throws Exception {
    Schema schema =
        Schema.builder()
            .addInt64Field("id")
            .addLogicalTypeField("payload", SqlTypes.VARIANT)
            .addNullableField("maybe", FieldType.logicalType(SqlTypes.VARIANT))
            .addArrayField("many", FieldType.logicalType(SqlTypes.VARIANT))
            .build();
    Row row =
        Row.withSchema(schema)
            .addValues(
                7L,
                PAYLOAD,
                null,
                java.util.Arrays.asList(Variant.parseJson("[1,2]"), Variant.parseJson("null")))
            .build();

    RowCoder coder = RowCoder.of(schema);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    coder.encode(row, out);
    Row decoded = coder.decode(new ByteArrayInputStream(out.toByteArray()));

    assertEquals(row, decoded);
    assertEquals(PAYLOAD, decoded.getLogicalTypeValue("payload", Variant.class));
    assertNull(decoded.getValue("maybe"));
    java.util.List<Variant> many =
        new java.util.ArrayList<>(checkArgumentNotNull(decoded.<Variant>getArray("many")));
    assertTrue(many.get(1).isVariantNull());
  }

  @Test
  public void testSchemaTranslationUsesPortableUrn() {
    SchemaApi.Schema proto = SchemaTranslation.schemaToProto(EVENT_SCHEMA, true);
    SchemaApi.LogicalType logicalType = proto.getFields(1).getType().getLogicalType();
    assertEquals("beam:logical_type:variant:v1", logicalType.getUrn());
    // SDKs that do not know the URN see ROW<metadata: BYTES, value: BYTES>.
    SchemaApi.Schema representation = logicalType.getRepresentation().getRowType().getSchema();
    assertEquals("metadata", representation.getFields(0).getName());
    assertEquals(SchemaApi.AtomicType.BYTES, representation.getFields(0).getType().getAtomicType());
    assertEquals("value", representation.getFields(1).getName());
    assertEquals(SchemaApi.AtomicType.BYTES, representation.getFields(1).getType().getAtomicType());
    // No Java class payload is needed for a standard logical type.
    assertTrue(logicalType.getPayload().isEmpty());

    Schema decoded = SchemaTranslation.schemaFromProto(proto);
    assertEquals(EVENT_SCHEMA, decoded);
    assertTrue(
        decoded.getField("payload").getType().getLogicalType() instanceof VariantLogicalType);
  }

  @Test
  public void testJsonToRowAndBack() {
    ObjectMapper reader = newObjectMapperWith(RowJsonDeserializer.forSchema(EVENT_SCHEMA));
    ObjectMapper writer = newObjectMapperWith(RowJsonSerializer.forSchema(EVENT_SCHEMA));

    // Rows with differently shaped payloads share one schema.
    Row login =
        jsonToRow(
            reader,
            "{\"id\": 1, \"payload\": {\"event\": \"login\", \"device\": {\"os\": \"ios\"}}}");
    Row purchase =
        jsonToRow(
            reader,
            "{\"id\": 2, \"payload\": {\"event\": \"purchase\", \"amount\": 19.99,"
                + " \"items\": [\"sku-1\", \"sku-2\"]}}");

    assertEquals(PAYLOAD, login.getLogicalTypeValue("payload", Variant.class));
    Variant purchasePayload = purchase.getLogicalTypeValue("payload", Variant.class);
    assertEquals(new java.math.BigDecimal("19.99"), purchasePayload.get("$.amount").getDecimal());
    assertEquals("sku-2", purchasePayload.get("$.items[1]").getString());

    // The payload is written back as nested JSON, not as a string.
    assertEquals(
        "{\"id\":2,\"payload\":{\"amount\":19.99,\"event\":\"purchase\",\"items\":[\"sku-1\",\"sku-2\"]}}",
        rowToJson(writer, purchase));
  }

  @Test
  public void testJsonToRowKeepsExactDecimals() {
    ObjectMapper reader = newObjectMapperWith(RowJsonDeserializer.forSchema(EVENT_SCHEMA));
    Row row = jsonToRow(reader, "{\"id\": 4, \"payload\": {\"amount\": 7.50, \"items\": [1, 2]}}");
    Variant payload = row.getLogicalTypeValue("payload", Variant.class);
    // Same bytes as Variant.parseJson: 7.50 stays an exact decimal with scale 2.
    assertEquals(new java.math.BigDecimal("7.50"), payload.get("$.amount").getDecimal());
    Variant expected = Variant.parseJson("{\"amount\": 7.50, \"items\": [1, 2]}");
    assertArrayEquals(expected.value(), payload.value());
  }

  @Test
  public void testJsonScalarPayload() {
    ObjectMapper reader = newObjectMapperWith(RowJsonDeserializer.forSchema(EVENT_SCHEMA));
    ObjectMapper writer = newObjectMapperWith(RowJsonSerializer.forSchema(EVENT_SCHEMA));
    Row row = jsonToRow(reader, "{\"id\": 3, \"payload\": \"just a string\"}");
    assertEquals("just a string", row.getLogicalTypeValue("payload", Variant.class).getString());
    assertEquals("{\"id\":3,\"payload\":\"just a string\"}", rowToJson(writer, row));
  }

  /** End to end: JSON lines -> Rows with a Variant column -> shuffle -> transform -> JSON. */
  @Test
  @Category(NeedsRunner.class)
  public void testPipelineJsonToRowAndToJson() {
    PCollection<Row> events =
        pipeline
            .apply(
                Create.of(
                    "{\"id\": 1, \"payload\": {\"event\": \"login\", \"device\": {\"os\": \"ios\"}}}",
                    "{\"id\": 2, \"payload\": {\"event\": \"purchase\", \"amount\": 19.99,"
                        + " \"items\": [\"sku-1\", \"sku-2\"]}}"))
            .apply(JsonToRow.withSchema(EVENT_SCHEMA));

    // Variant values survive coder round trips and user code.
    PCollection<String> eventNames =
        events.apply(
            "ExtractEvent",
            MapElements.into(TypeDescriptors.strings())
                .via(
                    row ->
                        checkArgumentNotNull(
                                checkArgumentNotNull(
                                        row.getLogicalTypeValue("payload", Variant.class))
                                    .get("$.event"))
                            .getString()));
    PAssert.that(eventNames).containsInAnyOrder("login", "purchase");

    PAssert.that(events.apply(ToJson.of()))
        .containsInAnyOrder(
            "{\"id\":1,\"payload\":{\"device\":{\"os\":\"ios\"},\"event\":\"login\"}}",
            "{\"id\":2,\"payload\":{\"amount\":19.99,\"event\":\"purchase\",\"items\":[\"sku-1\",\"sku-2\"]}}");

    pipeline.run().waitUntilFinish();
  }
}
