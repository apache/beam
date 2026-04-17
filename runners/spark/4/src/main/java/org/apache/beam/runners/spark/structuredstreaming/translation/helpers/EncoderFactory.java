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
package org.apache.beam.runners.spark.structuredstreaming.translation.helpers;

import static org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.emptyList;
import static org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.replace;
import static org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.seqOf;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal;
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder;
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders;
import org.apache.spark.sql.catalyst.encoders.AgnosticExpressionPathEncoder;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.expressions.BoundReference;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.objects.Invoke;
import org.apache.spark.sql.catalyst.expressions.objects.NewInstance;
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.collection.Iterator;
import scala.collection.immutable.Seq;
import scala.reflect.ClassTag;

public class EncoderFactory {
  // default constructor to reflectively create static invoke expressions
  private static final Constructor<StaticInvoke> STATIC_INVOKE_CONSTRUCTOR =
      (Constructor<StaticInvoke>) StaticInvoke.class.getConstructors()[0];

  private static final Constructor<Invoke> INVOKE_CONSTRUCTOR =
      (Constructor<Invoke>) Invoke.class.getConstructors()[0];

  private static final Constructor<NewInstance> NEW_INSTANCE_CONSTRUCTOR =
      (Constructor<NewInstance>) NewInstance.class.getConstructors()[0];

  @SuppressWarnings({"nullness", "unchecked"})
  static <T> ExpressionEncoder<T> create(
      Expression serializer, Expression deserializer, Class<? super T> clazz) {
    AgnosticEncoder<T> agnosticEncoder = new BeamAgnosticEncoder<>(serializer, deserializer, clazz);
    return ExpressionEncoder.apply(agnosticEncoder, serializer, deserializer);
  }

  /**
   * An {@link AgnosticEncoder} that implements both {@link AgnosticExpressionPathEncoder} (so that
   * {@code SerializerBuildHelper} / {@code DeserializerBuildHelper} delegate to our pre-built
   * expressions) and {@link AgnosticEncoders.StructEncoder} (so that {@code
   * Dataset.select(TypedColumn)} creates an N-attribute plan instead of a 1-attribute wrapped plan,
   * preventing {@code FIELD_NUMBER_MISMATCH} errors).
   *
   * <p>The {@code toCatalyst} / {@code fromCatalyst} methods substitute the {@code input}
   * expression into the pre-built serializer / deserializer via {@code transformUp}, so that when
   * this encoder is nested inside a composite encoder (e.g. {@code Encoders.tuple}) the correct
   * field-level expression is used in place of the root {@code BoundReference} / {@code
   * GetColumnByOrdinal}.
   */
  @SuppressWarnings({"nullness", "unchecked", "deprecation"})
  private static final class BeamAgnosticEncoder<T>
      implements AgnosticExpressionPathEncoder<T>, AgnosticEncoders.StructEncoder<T> {

    private final Expression serializer;
    private final Expression deserializer;
    private final Class<? super T> clazz;
    private final Seq<AgnosticEncoders.EncoderField> encoderFields;

    BeamAgnosticEncoder(Expression serializer, Expression deserializer, Class<? super T> clazz) {
      this.serializer = serializer;
      this.deserializer = deserializer;
      this.clazz = clazz;
      this.encoderFields = buildFields(serializer.dataType());
    }

    private static Seq<AgnosticEncoders.EncoderField> buildFields(DataType dt) {
      if (dt instanceof StructType) {
        StructField[] structFields = ((StructType) dt).fields();
        List<AgnosticEncoders.EncoderField> fields = new ArrayList<>(structFields.length);
        for (StructField sf : structFields) {
          fields.add(
              new AgnosticEncoders.EncoderField(
                  sf.name(),
                  new FieldEncoder<>(sf.dataType(), sf.nullable()),
                  sf.nullable(),
                  sf.metadata(),
                  Option.empty(),
                  Option.empty()));
        }
        return seqOf(fields.toArray(new AgnosticEncoders.EncoderField[0]));
      } else {
        // Non-struct: wrap in a single "value" field so StructEncoder sees one field.
        return seqOf(
            new AgnosticEncoders.EncoderField(
                "value",
                new FieldEncoder<>(dt, true),
                true,
                Metadata.empty(),
                Option.empty(),
                Option.empty()));
      }
    }

    // --- AgnosticExpressionPathEncoder ---

    @Override
    public Expression toCatalyst(Expression input) {
      return serializer.transformUp(replace(BoundReference.class, input));
    }

    @Override
    public Expression fromCatalyst(Expression input) {
      return deserializer.transformUp(replace(GetColumnByOrdinal.class, input));
    }

    // --- AgnosticEncoders.StructEncoder ---

    @Override
    public Seq<AgnosticEncoders.EncoderField> fields() {
      return encoderFields;
    }

    @Override
    public boolean isStruct() {
      return true;
    }

    @Override
    public void
        org$apache$spark$sql$catalyst$encoders$AgnosticEncoders$StructEncoder$_setter_$isStruct_$eq(
            boolean v) {
      // no-op: isStruct() is implemented directly above
    }

    // --- AgnosticEncoder / Encoder (explicit to resolve default-method ambiguity) ---

    @Override
    public boolean isPrimitive() {
      return false;
    }

    @Override
    public StructType schema() {
      // Build StructType from fields — mirrors the StructEncoder.schema() default.
      List<StructField> sfs = new ArrayList<>(encoderFields.size());
      Iterator<AgnosticEncoders.EncoderField> it = encoderFields.iterator();
      while (it.hasNext()) {
        sfs.add(it.next().structField());
      }
      return new StructType(sfs.toArray(new StructField[0]));
    }

    @Override
    public DataType dataType() {
      return schema();
    }

    @Override
    public ClassTag<T> clsTag() {
      return (ClassTag<T>) ClassTag.apply(clazz);
    }
  }

  /**
   * Minimal {@link AgnosticEncoder} stub used to carry per-field {@link DataType} metadata inside
   * {@link AgnosticEncoders.EncoderField}. The actual serialization / deserialization is handled by
   * {@link BeamAgnosticEncoder#toCatalyst} and {@link BeamAgnosticEncoder#fromCatalyst}.
   */
  @SuppressWarnings({"nullness", "unchecked"})
  private static final class FieldEncoder<V> implements AgnosticEncoder<V> {
    private final DataType fieldDataType;
    private final boolean fieldNullable;

    FieldEncoder(DataType dataType, boolean nullable) {
      this.fieldDataType = dataType;
      this.fieldNullable = nullable;
    }

    @Override
    public boolean isPrimitive() {
      return false;
    }

    @Override
    public DataType dataType() {
      return fieldDataType;
    }

    @Override
    public StructType schema() {
      return new StructType().add("value", fieldDataType, fieldNullable);
    }

    @Override
    public boolean nullable() {
      return fieldNullable;
    }

    @Override
    public ClassTag<V> clsTag() {
      return (ClassTag<V>) ClassTag.apply(Object.class);
    }
  }

  /**
   * Invoke method {@code fun} on Class {@code cls}, immediately propagating {@code null} if any
   * input arg is {@code null}.
   */
  static Expression invokeIfNotNull(Class<?> cls, String fun, DataType type, Expression... args) {
    return invoke(cls, fun, type, true, args);
  }

  /** Invoke method {@code fun} on Class {@code cls}. */
  static Expression invoke(Class<?> cls, String fun, DataType type, Expression... args) {
    return invoke(cls, fun, type, false, args);
  }

  private static Expression invoke(
      Class<?> cls, String fun, DataType type, boolean propagateNull, Expression... args) {
    try {
      // To address breaking interfaces between various versions of Spark, expressions are
      // created reflectively. This is fine as it's just needed once to create the query plan.
      switch (STATIC_INVOKE_CONSTRUCTOR.getParameterCount()) {
        case 6:
          // Spark 3.1.x
          return STATIC_INVOKE_CONSTRUCTOR.newInstance(
              cls, type, fun, seqOf(args), propagateNull, true);
        case 7:
          // Spark 3.2.0
          return STATIC_INVOKE_CONSTRUCTOR.newInstance(
              cls, type, fun, seqOf(args), emptyList(), propagateNull, true);
        case 8:
          // Spark 3.2.x, 3.3.x
          return STATIC_INVOKE_CONSTRUCTOR.newInstance(
              cls, type, fun, seqOf(args), emptyList(), propagateNull, true, true);
        case 9:
          // Spark 4.0.x: added Option<ScalarFunction<?>> parameter
          return STATIC_INVOKE_CONSTRUCTOR.newInstance(
              cls, type, fun, seqOf(args), emptyList(), propagateNull, true, true, Option.empty());
        default:
          throw new RuntimeException("Unsupported version of Spark");
      }
    } catch (IllegalArgumentException | ReflectiveOperationException ex) {
      throw new RuntimeException(ex);
    }
  }

  /** Invoke method {@code fun} on {@code obj} with provided {@code args}. */
  static Expression invoke(
      Expression obj, String fun, DataType type, boolean nullable, Expression... args) {
    try {
      // To address breaking interfaces between various versions of Spark, expressions are
      // created reflectively. This is fine as it's just needed once to create the query plan.
      switch (STATIC_INVOKE_CONSTRUCTOR.getParameterCount()) {
        case 6:
          // Spark 3.1.x
          return INVOKE_CONSTRUCTOR.newInstance(obj, fun, type, seqOf(args), false, nullable);
        case 7:
          // Spark 3.2.0
          return INVOKE_CONSTRUCTOR.newInstance(
              obj, fun, type, seqOf(args), emptyList(), false, nullable);
        case 8:
        case 9:
          // Spark 3.2.x, 3.3.x, 4.0.x: Invoke constructor is 8 params in all these versions
          return INVOKE_CONSTRUCTOR.newInstance(
              obj, fun, type, seqOf(args), emptyList(), false, nullable, true);
        default:
          throw new RuntimeException("Unsupported version of Spark");
      }
    } catch (IllegalArgumentException | ReflectiveOperationException ex) {
      throw new RuntimeException(ex);
    }
  }

  static Expression newInstance(Class<?> cls, DataType type, Expression... args) {
    try {
      // To address breaking interfaces between various versions of Spark, expressions are
      // created reflectively. This is fine as it's just needed once to create the query plan.
      switch (NEW_INSTANCE_CONSTRUCTOR.getParameterCount()) {
        case 5:
          return NEW_INSTANCE_CONSTRUCTOR.newInstance(cls, seqOf(args), true, type, Option.empty());
        case 6:
          // Spark 3.2.x, 3.3.x, 4.0.x: added immutable.Seq<AbstractDataType> parameter
          return NEW_INSTANCE_CONSTRUCTOR.newInstance(
              cls, seqOf(args), emptyList(), true, type, Option.empty());
        default:
          throw new RuntimeException("Unsupported version of Spark");
      }
    } catch (IllegalArgumentException | ReflectiveOperationException ex) {
      throw new RuntimeException(ex);
    }
  }
}
