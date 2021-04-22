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
package org.apache.beam.sdk.extensions.arrow;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.util.Text;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.CachingFactory;
import org.apache.beam.sdk.schemas.Factory;
import org.apache.beam.sdk.schemas.FieldValueGetter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.FixedBytes;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * Utilities to create {@link Iterable}s of Beam {@link Row} instances backed by Arrow record
 * batches.
 */
@Experimental(Experimental.Kind.SCHEMAS)
public class ArrowConversion {

  /** Get Beam Field from Arrow Field. */
  private static Field toBeamField(org.apache.arrow.vector.types.pojo.Field field) {
    FieldType beamFieldType = toFieldType(field.getFieldType(), field.getChildren());
    return Field.of(field.getName(), beamFieldType);
  }

  /** Converts Arrow FieldType to Beam FieldType. */
  private static FieldType toFieldType(
      org.apache.arrow.vector.types.pojo.FieldType arrowFieldType,
      List<org.apache.arrow.vector.types.pojo.Field> childrenFields) {
    FieldType fieldType =
        arrowFieldType
            .getType()
            .accept(
                new ArrowType.ArrowTypeVisitor<FieldType>() {
                  @Override
                  public FieldType visit(ArrowType.Null type) {
                    throw new IllegalArgumentException(
                        "Type \'" + type.toString() + "\' not supported.");
                  }

                  @Override
                  public FieldType visit(ArrowType.Struct type) {
                    return FieldType.row(ArrowSchemaTranslator.toBeamSchema(childrenFields));
                  }

                  @Override
                  public FieldType visit(ArrowType.List type) {
                    checkArgument(
                        childrenFields.size() == 1,
                        "Encountered "
                            + childrenFields.size()
                            + " child fields for list type, expected 1");
                    return FieldType.array(toBeamField(childrenFields.get(0)).getType());
                  }

                  @Override
                  public FieldType visit(ArrowType.FixedSizeList type) {
                    throw new IllegalArgumentException(
                        "Type \'" + type.toString() + "\' not supported.");
                  }

                  @Override
                  public FieldType visit(ArrowType.Union type) {
                    throw new IllegalArgumentException(
                        "Type \'" + type.toString() + "\' not supported.");
                  }

                  @Override
                  public FieldType visit(ArrowType.Map type) {
                    checkArgument(
                        childrenFields.size() == 2,
                        "Encountered "
                            + childrenFields.size()
                            + " child fields for map type, expected 2");
                    return FieldType.map(
                        toBeamField(childrenFields.get(0)).getType(),
                        toBeamField(childrenFields.get(1)).getType());
                  }

                  @Override
                  public FieldType visit(ArrowType.Int type) {
                    if (!type.getIsSigned()) {
                      throw new IllegalArgumentException("Unsigned integers are not supported.");
                    }
                    switch (type.getBitWidth()) {
                      case 8:
                        return FieldType.BYTE;
                      case 16:
                        return FieldType.INT16;
                      case 32:
                        return FieldType.INT32;
                      case 64:
                        return FieldType.INT64;
                      default:
                        throw new IllegalArgumentException(
                            "Unsupported integer bit width: " + type.getBitWidth());
                    }
                  }

                  @Override
                  public FieldType visit(ArrowType.FloatingPoint type) {
                    switch (type.getPrecision()) {
                      case SINGLE:
                        return FieldType.FLOAT;
                      case DOUBLE:
                        return FieldType.DOUBLE;
                      default:
                        throw new IllegalArgumentException(
                            "Unsupported floating-point precision: " + type.getPrecision().name());
                    }
                  }

                  @Override
                  public FieldType visit(ArrowType.Utf8 type) {
                    return FieldType.STRING;
                  }

                  @Override
                  public FieldType visit(ArrowType.Binary type) {
                    return FieldType.BYTES;
                  }

                  @Override
                  public FieldType visit(ArrowType.FixedSizeBinary type) {
                    return FieldType.logicalType(FixedBytes.of(type.getByteWidth()));
                  }

                  @Override
                  public FieldType visit(ArrowType.Bool type) {
                    return FieldType.BOOLEAN;
                  }

                  @Override
                  public FieldType visit(ArrowType.Decimal type) {
                    // FieldType.DECIMAL isn't perfect here since arrow decimal has a
                    // scale/precision fixed by the schema, but FieldType.DECIMAL uses a BigDecimal,
                    // whose precision/scale can change from row to row.
                    throw new IllegalArgumentException(
                        "Type \'" + type.toString() + "\' not supported.");
                  }

                  @Override
                  public FieldType visit(ArrowType.Date type) {
                    throw new IllegalArgumentException(
                        "Type \'" + type.toString() + "\' not supported.");
                  }

                  @Override
                  public FieldType visit(ArrowType.Time type) {
                    throw new IllegalArgumentException(
                        "Type \'" + type.toString() + "\' not supported.");
                  }

                  @Override
                  public FieldType visit(ArrowType.Timestamp type) {
                    if (type.getUnit() == TimeUnit.MILLISECOND
                        || type.getUnit() == TimeUnit.MICROSECOND) {
                      return FieldType.DATETIME;
                    } else {
                      throw new IllegalArgumentException(
                          "Unsupported timestamp unit: " + type.getUnit().name());
                    }
                  }

                  @Override
                  public FieldType visit(ArrowType.Interval type) {
                    throw new IllegalArgumentException(
                        "Type \'" + type.toString() + "\' not supported.");
                  }

                  @Override
                  public FieldType visit(ArrowType.Duration type) {
                    throw new IllegalArgumentException(
                        "Type \'" + type.toString() + "\' not supported.");
                  }

                  @Override
                  public FieldType visit(ArrowType.LargeBinary type) {
                    throw new IllegalArgumentException(
                        "Type \'" + type.toString() + "\' not supported.");
                  }

                  @Override
                  public FieldType visit(ArrowType.LargeUtf8 type) {
                    throw new IllegalArgumentException(
                        "Type \'" + type.toString() + "\' not supported.");
                  }

                  @Override
                  public FieldType visit(ArrowType.LargeList type) {
                    throw new IllegalArgumentException(
                        "Type \'" + type.toString() + "\' not supported.");
                  }
                });
    return fieldType.withNullable(arrowFieldType.isNullable());
  }

  /**
   * Returns an {@link Iterable<Row>} backed by the Arrow record batch stored in {@code
   * vectorSchemaRoot}.
   *
   * <p>Note this is a lazy interface. The data in the underlying Arrow buffer is not read until a
   * field of one of the returned {@link Row}s is accessed.
   */
  public static Iterable<Row> rowsFromRecordBatch(
      Schema schema, VectorSchemaRoot vectorSchemaRoot) {
    return new RecordBatchIterable(schema, vectorSchemaRoot);
  }

  public static Iterable<Row> rowsFromRecordBatch(VectorSchemaRoot vectorSchemaRoot) {
    return rowsFromRecordBatch(
        ArrowSchemaTranslator.toBeamSchema(vectorSchemaRoot.getSchema()), vectorSchemaRoot);
  }

  @SuppressWarnings("rawtypes")
  private static class VectorSchemaRootRowIterator implements Iterator<Row> {
    private static final ArrowValueConverterVisitor valueConverterVisitor =
        new ArrowValueConverterVisitor();
    private final Schema schema;
    private final VectorSchemaRoot vectorSchemaRoot;
    private final Factory<List<FieldValueGetter>> fieldValueGetters;
    private Integer currRowIndex;

    private static class FieldVectorListValueGetterFactory
        implements Factory<List<FieldValueGetter>> {
      private final List<FieldVector> fieldVectors;

      static FieldVectorListValueGetterFactory of(List<FieldVector> fieldVectors) {
        return new FieldVectorListValueGetterFactory(fieldVectors);
      }

      private FieldVectorListValueGetterFactory(List<FieldVector> fieldVectors) {
        this.fieldVectors = fieldVectors;
      }

      @Override
      public List<FieldValueGetter> create(Class<?> clazz, Schema schema) {
        return this.fieldVectors.stream()
            .map(
                (fieldVector) -> {
                  Function<Object, Object> conversionFunction =
                      fieldVector.getField().getFieldType().getType().accept(valueConverterVisitor);
                  if (conversionFunction == null) {
                    return new FieldValueGetter<Integer, Object>() {
                      @Nullable
                      @Override
                      public Object get(Integer rowIndex) {
                        return fieldVector.getObject(rowIndex);
                      }

                      @Override
                      public String name() {
                        return fieldVector.getField().getName();
                      }
                    };
                  } else {
                    return new FieldValueGetter<Integer, Object>() {
                      @Nullable
                      @Override
                      public Object get(Integer rowIndex) {
                        Object value = fieldVector.getObject(rowIndex);
                        if (value == null) {
                          return null;
                        }

                        return conversionFunction.apply(value);
                      }

                      @Override
                      public String name() {
                        return fieldVector.getField().getName();
                      }
                    };
                  }
                })
            .collect(Collectors.toList());
      }
    }

    // TODO: Consider using ByteBuddyUtils.TypeConversion for this
    private static class ArrowValueConverterVisitor
        implements ArrowType.ArrowTypeVisitor<Function<Object, Object>> {
      @Override
      public Function<Object, Object> visit(ArrowType.Null type) {
        throw new IllegalArgumentException("Type \'" + type.toString() + "\' not supported.");
      }

      @Override
      public @Nullable Function<Object, Object> visit(ArrowType.Struct type) {
        // TODO: code to create a row.
        return null;
      }

      @Override
      public Function<Object, Object> visit(ArrowType.List type) {
        return null;
      }

      @Override
      public Function<Object, Object> visit(ArrowType.FixedSizeList type) {
        throw new IllegalArgumentException("Type \'" + type.toString() + "\' not supported.");
      }

      @Override
      public Function<Object, Object> visit(ArrowType.Union type) {
        throw new IllegalArgumentException("Type \'" + type.toString() + "\' not supported.");
      }

      @Override
      public Function<Object, Object> visit(ArrowType.Map type) {
        throw new IllegalArgumentException("Type \'" + type.toString() + "\' not supported.");
      }

      @Override
      public Function<Object, Object> visit(ArrowType.Duration type) {
        throw new IllegalArgumentException("Type \'" + type.toString() + "\' not supported.");
      }

      @Override
      public Function<Object, Object> visit(ArrowType.Int type) {
        return null;
      }

      @Override
      public Function<Object, Object> visit(ArrowType.FloatingPoint type) {
        return null;
      }

      @Override
      public Function<Object, Object> visit(ArrowType.Utf8 type) {
        return (Object text) -> ((Text) text).toString();
      }

      @Override
      public Function<Object, Object> visit(ArrowType.Binary type) {
        return null;
      }

      @Override
      public Function<Object, Object> visit(ArrowType.FixedSizeBinary type) {
        return null;
      }

      @Override
      public Function<Object, Object> visit(ArrowType.Bool type) {
        return null;
      }

      @Override
      public Function<Object, Object> visit(ArrowType.Decimal type) {
        throw new IllegalArgumentException("Type \'" + type.toString() + "\' not supported.");
      }

      @Override
      public Function<Object, Object> visit(ArrowType.Date type) {
        throw new IllegalArgumentException("Type \'" + type.toString() + "\' not supported.");
      }

      @Override
      public Function<Object, Object> visit(ArrowType.Time type) {
        throw new IllegalArgumentException("Type \'" + type.toString() + "\' not supported.");
      }

      @Override
      public Function<Object, Object> visit(ArrowType.Timestamp type) {
        DateTimeZone tz;
        try {
          tz = DateTimeZone.forID(type.getTimezone());
        } catch (Exception e) {
          throw new IllegalArgumentException(
              "Encountered unrecognized Timezone: " + type.getTimezone());
        }
        switch (type.getUnit()) {
          case MICROSECOND:
            return (epochMicros) -> new DateTime((long) epochMicros / 1000, tz);
          case MILLISECOND:
            return (epochMills) -> new DateTime((long) epochMills, tz);
          default:
            throw new AssertionError("Encountered unrecognized TimeUnit: " + type.getUnit());
        }
      }

      @Override
      public Function<Object, Object> visit(ArrowType.Interval type) {
        throw new IllegalArgumentException("Type \'" + type.toString() + "\' not supported.");
      }

      @Override
      public Function<Object, Object> visit(ArrowType.LargeBinary type) {
        throw new IllegalArgumentException("Type \'" + type.toString() + "\' not supported.");
      }

      @Override
      public Function<Object, Object> visit(ArrowType.LargeUtf8 type) {
        throw new IllegalArgumentException("Type \'" + type.toString() + "\' not supported.");
      }

      @Override
      public Function<Object, Object> visit(ArrowType.LargeList type) {
        throw new IllegalArgumentException("Type \'" + type.toString() + "\' not supported.");
      }
    }

    public VectorSchemaRootRowIterator(Schema schema, VectorSchemaRoot vectorSchemaRoot) {
      this.schema = schema;
      this.vectorSchemaRoot = vectorSchemaRoot;
      this.fieldValueGetters =
          new CachingFactory<>(
              FieldVectorListValueGetterFactory.of(vectorSchemaRoot.getFieldVectors()));
      this.currRowIndex = 0;
    }

    @Override
    public boolean hasNext() {
      return currRowIndex < vectorSchemaRoot.getRowCount();
    }

    @Override
    public Row next() {
      if (!hasNext()) {
        throw new IllegalStateException("There are no more Rows.");
      }
      Row result =
          Row.withSchema(schema).withFieldValueGetters(this.fieldValueGetters, this.currRowIndex);
      this.currRowIndex += 1;
      return result;
    }
  }

  private static class RecordBatchIterable implements Iterable<Row> {
    private final Schema schema;
    private final VectorSchemaRoot vectorSchemaRoot;

    RecordBatchIterable(Schema schema, VectorSchemaRoot vectorSchemaRoot) {
      this.schema = schema;
      this.vectorSchemaRoot = vectorSchemaRoot;
    }

    @Override
    public Iterator<Row> iterator() {
      return new VectorSchemaRootRowIterator(schema, vectorSchemaRoot);
    }
  }

  private ArrowConversion() {}

  /** Converts Arrow schema to Beam row schema. */
  public static class ArrowSchemaTranslator {

    public static Schema toBeamSchema(org.apache.arrow.vector.types.pojo.Schema schema) {
      return toBeamSchema(schema.getFields());
    }

    public static Schema toBeamSchema(List<org.apache.arrow.vector.types.pojo.Field> fields) {
      Schema.Builder builder = Schema.builder();
      for (org.apache.arrow.vector.types.pojo.Field field : fields) {
        Field beamField = toBeamField(field);
        builder.addField(beamField);
      }
      return builder.build();
    }
  }
}
