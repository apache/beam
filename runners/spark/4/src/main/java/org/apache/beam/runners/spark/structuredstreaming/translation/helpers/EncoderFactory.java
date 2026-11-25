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
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.collection.Iterator;
import scala.collection.immutable.Seq;
import scala.reflect.ClassTag;

public class EncoderFactory {
  // Resolve the Scala case-class primary constructor (the one with the most parameters).
  // Constructor ordering returned by Class.getConstructors() is JVM-defined and not stable
  // across Spark versions, so we pick the widest constructor explicitly and then dispatch on
  // parameter count below to pick the right argument shape per Spark version.
  private static final Constructor<StaticInvoke> STATIC_INVOKE_CONSTRUCTOR =
      primaryConstructor(StaticInvoke.class);

  private static final Constructor<Invoke> INVOKE_CONSTRUCTOR = primaryConstructor(Invoke.class);

  private static final Constructor<NewInstance> NEW_INSTANCE_CONSTRUCTOR =
      primaryConstructor(NewInstance.class);

  @SuppressWarnings("unchecked")
  private static <T> Constructor<T> primaryConstructor(Class<T> cls) {
    Constructor<?>[] ctors = cls.getConstructors();
    Constructor<?> widest = ctors[0];
    for (int i = 1; i < ctors.length; i++) {
      if (ctors[i].getParameterCount() > widest.getParameterCount()) {
        widest = ctors[i];
      }
    }
    return (Constructor<T>) widest;
  }

  @SuppressWarnings({"nullness", "unchecked"})
  static <T> ExpressionEncoder<T> create(
      Expression serializer, Expression deserializer, Class<? super T> clazz) {
    AgnosticEncoder<T> agnosticEncoder =
        serializer.dataType() instanceof StructType
            ? new StructBeamAgnosticEncoder<>(serializer, deserializer, clazz)
            : new BeamAgnosticEncoder<>(serializer, deserializer, clazz);
    return ExpressionEncoder.apply(agnosticEncoder, serializer, deserializer);
  }

  /**
   * An {@link AgnosticEncoder} that implements {@link AgnosticExpressionPathEncoder} (so that
   * {@code SerializerBuildHelper} / {@code DeserializerBuildHelper} delegate to our pre-built
   * expressions). {@code isStruct} must match the serializer because {@code
   * TypedAggUtils.aggKeyColumn} and {@code Dataset.select(TypedColumn)} branch on it.
   *
   * <p>The {@code toCatalyst} / {@code fromCatalyst} methods substitute the {@code input}
   * expression into the pre-built serializer / deserializer via {@code transformUp}, so that when
   * this encoder is nested inside a composite encoder (e.g. {@code Encoders.tuple}) the correct
   * field-level expression is used in place of the root {@code BoundReference} / {@code
   * GetColumnByOrdinal}.
   */
  @SuppressWarnings({"nullness", "unchecked", "deprecation"})
  private static class BeamAgnosticEncoder<T> implements AgnosticExpressionPathEncoder<T> {

    private final Expression serializer;
    private final Expression deserializer;
    private final Class<? super T> clazz;

    BeamAgnosticEncoder(Expression serializer, Expression deserializer, Class<? super T> clazz) {
      this.serializer = serializer;
      this.deserializer = deserializer;
      this.clazz = clazz;
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

    @Override
    public boolean isPrimitive() {
      return false;
    }

    @Override
    public DataType dataType() {
      return serializer.dataType();
    }

    @Override
    public ClassTag<T> clsTag() {
      return (ClassTag<T>) ClassTag.apply(clazz);
    }
  }

  /**
   * {@link BeamAgnosticEncoder} that implements {@link AgnosticEncoders.StructEncoder} for {@link
   * StructType} serializers (so that {@code Dataset.select(TypedColumn)} creates an N-attribute
   * plan instead of a 1-attribute wrapped plan, preventing {@code FIELD_NUMBER_MISMATCH} errors).
   */
  private static final class StructBeamAgnosticEncoder<T> extends BeamAgnosticEncoder<T>
      implements AgnosticEncoders.StructEncoder<T> {

    private final Seq<AgnosticEncoders.EncoderField> encoderFields;

    StructBeamAgnosticEncoder(
        Expression serializer, Expression deserializer, Class<? super T> clazz) {
      super(serializer, deserializer, clazz);
      StructField[] structFields = ((StructType) serializer.dataType()).fields();
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
      this.encoderFields = seqOf(fields.toArray(new AgnosticEncoders.EncoderField[0]));
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

    /** Scala trait {@code val} setter, required to implement {@code StructEncoder} from Java. */
    @Override
    public void
        org$apache$spark$sql$catalyst$encoders$AgnosticEncoders$StructEncoder$_setter_$isStruct_$eq(
            boolean v) {}

    // --- AgnosticEncoder / Encoder (explicit to resolve default-method ambiguity) ---

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
      switch (INVOKE_CONSTRUCTOR.getParameterCount()) {
        case 6:
          // Spark 3.1.x
          return INVOKE_CONSTRUCTOR.newInstance(obj, fun, type, seqOf(args), false, nullable);
        case 7:
          // Spark 3.2.0
          return INVOKE_CONSTRUCTOR.newInstance(
              obj, fun, type, seqOf(args), emptyList(), false, nullable);
        case 8:
          // Spark 3.2.x, 3.3.x, 4.0.x: Invoke constructor is 8 params across all these versions
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
