package org.apache.beam.sdk.extensions.sbe;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.apache.beam.sdk.extensions.sbe.SbeLogicalTypes.Uint8;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class SbeSchemaTest {

  private static final Field STRING_FIELD = Field.of("stringField", FieldType.STRING);
  private static final Field UINT_8_FIELD = Field.of("uint8Field", FieldType.logicalType(new Uint8()));

  @Test
  public void testNoSchemaGenerationValid() {
    SbeSchema sbeSchema = SbeSchema.builder()
        .addField(STRING_FIELD)
        .insertField(1, UINT_8_FIELD)
        .build();

    Schema expectedSchema = Schema.builder()
        .addFields(STRING_FIELD, UINT_8_FIELD)
        .build();

    assertEquals(expectedSchema, sbeSchema.schema());
  }

  @Test
  public void testNoSchemaGenerationEmpty() {
    assertEquals(Schema.builder().build(), SbeSchema.builder().build().schema());
  }

  @Test
  public void testNoSchemaGenerationInvalid() {
    SbeSchema.Builder builder = SbeSchema.builder().insertField(1, STRING_FIELD);
    assertThrows(IllegalStateException.class, builder::build);
  }
}