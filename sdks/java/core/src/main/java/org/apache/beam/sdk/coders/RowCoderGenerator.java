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
package org.apache.beam.sdk.coders;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.util.StringUtils;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Creates and caches a {@link Coder} for {@link Row} objects corresponding to a schema. */
@SuppressWarnings({
  "nullness", // TODO(https://github.com/apache/beam/issues/20497)
  "rawtypes"
})
public abstract class RowCoderGenerator {
  private static final BitSetCoder NULL_LIST_CODER = BitSetCoder.of();
  private static final VarIntCoder VAR_INT_CODER = VarIntCoder.of();
  // BitSet.get(n) will return false for any n >= nbits, so a BitSet with 0 bits will return false
  // for all calls to get.
  private static final BitSet EMPTY_BIT_SET = new BitSet(0);

  private static final String SCHEMA_OPTION_STATIC_ENCODING = "beam:option:row:static_encoding";

  static class WithStackTrace<T> {
    private final T value;
    private final String stackTrace;

    public WithStackTrace(T value, String stackTrace) {
      this.value = value;
      this.stackTrace = stackTrace;
    }

    public T getValue() {
      return value;
    }

    public String getStackTrace() {
      return stackTrace;
    }
  }

  // Cache for coders that are already created.
  @GuardedBy("cacheLock")
  private static final Map<UUID, WithStackTrace<Coder<Row>>> GENERATED_CODERS = Maps.newHashMap();

  @GuardedBy("cacheLock")
  private static final Map<UUID, WithStackTrace<Map<String, Integer>>> ENCODING_POSITION_OVERRIDES =
      Maps.newHashMap();

  private static final Object cacheLock = new Object();

  private static final Logger LOG = LoggerFactory.getLogger(RowCoderGenerator.class);

  private static String getStackTrace() {
    return StringUtils.arrayToNewlines(Thread.currentThread().getStackTrace(), 10);
  }

  public static void overrideEncodingPositions(UUID uuid, Map<String, Integer> encodingPositions) {
    final String stackTrace = getStackTrace();
    synchronized (cacheLock) {
      @Nullable
      WithStackTrace<Map<String, Integer>> previousEncodingPositions =
          ENCODING_POSITION_OVERRIDES.put(
              uuid, new WithStackTrace<>(encodingPositions, stackTrace));
      @Nullable WithStackTrace<Coder<Row>> existingCoder = GENERATED_CODERS.get(uuid);
      if (previousEncodingPositions == null) {
        if (existingCoder != null) {
          LOG.error(
              "Received encoding positions for uuid {} too late after creating RowCoder. Created: {}\n Override: {}",
              uuid,
              existingCoder.getStackTrace(),
              stackTrace);
        } else {
          LOG.info("Received encoding positions {} for uuid {}.", encodingPositions, uuid);
        }
      } else if (!previousEncodingPositions.getValue().equals(encodingPositions)) {
        if (existingCoder == null) {
          LOG.error(
              "Received differing encoding positions for uuid {} before coder creation. Was {} at {}\n Now {} at {}",
              uuid,
              previousEncodingPositions.getValue(),
              encodingPositions,
              previousEncodingPositions.getStackTrace(),
              stackTrace);
        } else {
          LOG.error(
              "Received differing encoding positions for uuid {} after coder creation at {}\n. "
                  + "Was {} at {}\n Now {} at {}\n",
              uuid,
              existingCoder.getStackTrace(),
              previousEncodingPositions.getValue(),
              encodingPositions,
              previousEncodingPositions.getStackTrace(),
              stackTrace);
        }
      }
    }
  }

  @VisibleForTesting
  static void clearRowCoderCache() {
    synchronized (cacheLock) {
      GENERATED_CODERS.clear();
    }
  }

  public static Coder<Row> generate(Schema schema) {
    UUID uuid = Preconditions.checkNotNull(schema.getUUID());
    // Avoid using computeIfAbsent which may cause issues with nested schemas.
    synchronized (cacheLock) {
      @Nullable WithStackTrace<Coder<Row>> existingRowCoder = GENERATED_CODERS.get(uuid);
      if (existingRowCoder != null) {
        return existingRowCoder.getValue();
      }
      int[] encodingPosToRowIndex = new int[schema.getFieldCount()];
      @Nullable
      WithStackTrace<Map<String, Integer>> existingEncodingPositions =
          ENCODING_POSITION_OVERRIDES.get(uuid);
      Map<String, Integer> encodingPositions =
          existingEncodingPositions == null
              ? schema.getEncodingPositions()
              : existingEncodingPositions.getValue();
      for (int recordIndex = 0; recordIndex < schema.getFieldCount(); ++recordIndex) {
        String name = schema.getField(recordIndex).getName();
        int encodingPosition = encodingPositions.get(name);
        encodingPosToRowIndex[encodingPosition] = recordIndex;
      }
      // There should never be duplicate encoding positions.
      Preconditions.checkState(
          schema.getFieldCount() == Arrays.stream(encodingPosToRowIndex).distinct().count());

      // Component coders are ordered by encoding position, but may encode a field with a different
      // row index.
      Coder[] componentCoders = new Coder[schema.getFieldCount()];
      for (int i = 0; i < schema.getFieldCount(); ++i) {
        int rowIndex = encodingPosToRowIndex[i];
        // We use withNullable(false) as nulls are handled by the RowCoder and the individual
        // component coders therefore do not need to handle nulls.
        componentCoders[i] =
            SchemaCoder.coderForFieldType(schema.getField(rowIndex).getType().withNullable(false));
      }

      Coder<Row> rowCoder =
          new RowCoderImpl(
              schema,
              componentCoders,
              encodingPosToRowIndex,
              schema.getFields().stream().map(Field::getType).anyMatch(FieldType::getNullable));
      String stackTrace = getStackTrace();
      GENERATED_CODERS.put(uuid, new WithStackTrace<>(rowCoder, stackTrace));
      LOG.debug(
          "Created row coder for uuid {} with encoding positions {} at {}",
          uuid,
          encodingPositions,
          stackTrace);
      return rowCoder;
    }
  }

  private static final class RowCoderImpl extends CustomCoder<Row> {
    private final Schema schema;
    private final Coder[] coders;
    private final int[] encodingPosToIndex;
    private final boolean hasNullableFields;

    private RowCoderImpl(
        Schema schema, Coder[] coders, int[] encodingPosToIndex, boolean hasNullableFields) {
      this.schema = schema;
      this.coders = coders;
      this.encodingPosToIndex = encodingPosToIndex;
      this.hasNullableFields = hasNullableFields;
    }

    @Override
    public void encode(Row value, OutputStream outputStream) throws IOException {
      encodeDelegate(coders, encodingPosToIndex, value, outputStream, hasNullableFields);
    }

    @Override
    public Row decode(InputStream inputStream) throws IOException {
      return decodeDelegate(schema, coders, encodingPosToIndex, inputStream);
    }

    @SuppressWarnings("unchecked")
    private static void encodeDelegate(
        Coder[] coders,
        int[] encodingPosToIndex,
        Row value,
        OutputStream outputStream,
        boolean hasNullableFields)
        throws IOException {
      checkState(value.getFieldCount() == value.getSchema().getFieldCount());
      checkState(encodingPosToIndex.length == value.getFieldCount());

      boolean staticEncoding =
          value.getSchema().getOptions().getValueOrDefault(SCHEMA_OPTION_STATIC_ENCODING, false);

      // Encode the field count. This allows us to handle compatible schema changes.
      if (!staticEncoding) {
        VAR_INT_CODER.encode(value.getFieldCount(), outputStream);
      }

      if (hasNullableFields) {
        // If the row has null fields, extract the values out once so that both scanNullFields and
        // the encoding can share it and avoid having to extract them twice.

        Object[] fieldValues = new Object[value.getFieldCount()];
        for (int idx = 0; idx < fieldValues.length; ++idx) {
          fieldValues[idx] = value.getValue(idx);
        }

        // Encode a bitmap for the null fields to save having to encode a bunch of nulls.
        if (!staticEncoding) {
          NULL_LIST_CODER.encode(scanNullFields(fieldValues, encodingPosToIndex), outputStream);
        }
        for (int encodingPos = 0; encodingPos < fieldValues.length; ++encodingPos) {
          @Nullable Object fieldValue = fieldValues[encodingPosToIndex[encodingPos]];
          if (fieldValue != null) {
            coders[encodingPos].encode(fieldValue, outputStream);
          }
        }
      } else {
        // Otherwise, we know all fields are non-null, so the null list is always empty.

        if (!staticEncoding) {
          NULL_LIST_CODER.encode(EMPTY_BIT_SET, outputStream);
        }
        for (int encodingPos = 0; encodingPos < value.getFieldCount(); ++encodingPos) {
          @Nullable Object fieldValue = value.getValue(encodingPosToIndex[encodingPos]);
          if (fieldValue != null) {
            coders[encodingPos].encode(fieldValue, outputStream);
          }
        }
      }
    }

    // Figure out which fields of the Row are null, and returns a BitSet. This allows us to save
    // on encoding each null field separately.
    private static BitSet scanNullFields(Object[] fieldValues, int[] encodingPosToIndex) {
      Preconditions.checkState(fieldValues.length == encodingPosToIndex.length);
      BitSet nullFields = new BitSet(fieldValues.length);
      for (int encodingPos = 0; encodingPos < encodingPosToIndex.length; ++encodingPos) {
        int fieldIndex = encodingPosToIndex[encodingPos];
        if (fieldValues[fieldIndex] == null) {
          nullFields.set(encodingPos);
        }
      }
      return nullFields;
    }

    private static Row decodeDelegate(
        Schema schema, Coder[] coders, int[] encodingPosToIndex, InputStream inputStream)
        throws IOException {
      int fieldCount;
      BitSet nullFields;
      if (schema.getOptions().getValueOrDefault(SCHEMA_OPTION_STATIC_ENCODING, false)) {
        fieldCount = schema.getFieldCount();
        nullFields = new BitSet();
      } else {
        fieldCount = VAR_INT_CODER.decode(inputStream);
        nullFields = NULL_LIST_CODER.decode(inputStream);
      }
      Object[] fieldValues = new Object[coders.length];
      for (int encodingPos = 0; encodingPos < fieldCount; ++encodingPos) {
        // In the case of a schema change going backwards, fieldCount might be > coders.length,
        // in which case we drop the extra fields.
        if (encodingPos < coders.length) {
          int rowIndex = encodingPosToIndex[encodingPos];
          if (nullFields.get(encodingPos)) {
            fieldValues[rowIndex] = null;
          } else {
            Object fieldValue = coders[encodingPos].decode(inputStream);
            fieldValues[rowIndex] = fieldValue;
          }
        }
      }
      // If the schema was evolved to contain more fields, we fill them in with nulls.
      for (int encodingPos = fieldCount; encodingPos < coders.length; encodingPos++) {
        int rowIndex = encodingPosToIndex[encodingPos];
        fieldValues[rowIndex] = null;
      }
      // We call attachValues instead of setValues. setValues validates every element in the list
      // is of the proper type, potentially converts to the internal type Row stores, and copies
      // all values. Since we assume that decode is always being called on a previously-encoded
      // Row, the values should already be validated and of the correct type. So, we can save
      // some processing by simply transferring ownership of the list to the Row.
      return Row.withSchema(schema).attachValues(fieldValues);
    }
  }
}
