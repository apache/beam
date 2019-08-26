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

import static org.apache.spark.sql.types.DataTypes.BinaryType;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.beam.runners.spark.structuredstreaming.translation.SchemaHelpers;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.expressions.BoundReference;
import org.apache.spark.sql.catalyst.expressions.Cast;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NonSQLExpression;
import org.apache.spark.sql.catalyst.expressions.UnaryExpression;
import org.apache.spark.sql.catalyst.expressions.codegen.Block;
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator;
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext;
import org.apache.spark.sql.catalyst.expressions.codegen.ExprCode;
import org.apache.spark.sql.catalyst.expressions.codegen.ExprValue;
import org.apache.spark.sql.catalyst.expressions.codegen.SimpleExprValue;
import org.apache.spark.sql.catalyst.expressions.codegen.VariableValue;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.ObjectType;
import scala.StringContext;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

/** {@link Encoders} utility class. */
public class EncoderHelpers {

  // 1. use actual class and not object to avoid Spark fallback to GenericRowWithSchema.
  // 2. use raw class because only raw classes can be used with kryo. Cast to Class<T> to allow
  // the type inference mechanism to infer for ex Encoder<WindowedValue<T>> to get back the type
  // checking

  /*
   --------- Encoders for internal spark runner objects
  */

  /**
   * Get a bytes {@link Encoder} for {@link WindowedValue}. Bytes serialisation is issued by Kryo
   */
  @SuppressWarnings("unchecked")
  public static <T> Encoder<T> windowedValueEncoder() {
    return Encoders.kryo((Class<T>) WindowedValue.class);
  }

  /** Get a bytes {@link Encoder} for {@link KV}. Bytes serialisation is issued by Kryo */
  @SuppressWarnings("unchecked")
  public static <T> Encoder<T> kvEncoder() {
    return Encoders.kryo((Class<T>) KV.class);
  }

  /** Get a bytes {@link Encoder} for {@code T}. Bytes serialisation is issued by Kryo */
  @SuppressWarnings("unchecked")
  public static <T> Encoder<T> genericEncoder() {
    return Encoders.kryo((Class<T>) Object.class);
  }

  /** Get a bytes {@link Encoder} for {@link Tuple2}. Bytes serialisation is issued by Kryo */
  public static <T1, T2> Encoder<Tuple2<T1, T2>> tuple2Encoder() {
    return Encoders.tuple(EncoderHelpers.genericEncoder(), EncoderHelpers.genericEncoder());
  }

  /*
   --------- Bridges from Beam Coders to Spark Encoders
  */

  /** A way to construct encoders using generic serializers. */
  private <T> Encoder<T> fromBeamCoder(Coder<T> coder, Class<T> claz){

    List<Expression> serialiserList = new ArrayList<>();
    serialiserList.add(new EncodeUsingBeamCoder<>(claz, coder));
    ClassTag<T> classTag = ClassTag$.MODULE$.apply(claz);
    return new ExpressionEncoder<>(
        SchemaHelpers.binarySchema(),
        false,
        JavaConversions.collectionAsScalaIterable(serialiserList).toSeq(),
        new DecodeUsingBeamCoder<>(classTag, coder), classTag);

/*
    ExpressionEncoder[T](
        schema = new StructType().add("value", BinaryType),
        flat = true,
        serializer = Seq(
            EncodeUsingSerializer(
                BoundReference(0, ObjectType(classOf[AnyRef]), nullable = true), kryo = useKryo)),
        deserializer =
            DecodeUsingSerializer[T](
        Cast(GetColumnByOrdinal(0, BinaryType), BinaryType),
        classTag[T],
        kryo = useKryo),
    clsTag = classTag[T]
    )
*/
  }

  private static class EncodeUsingBeamCoder<T> extends UnaryExpression implements NonSQLExpression {

    private Class<T> claz;
    private Coder<T> beamCoder;
    private Expression child;

    private EncodeUsingBeamCoder( Class<T> claz, Coder<T> beamCoder) {
      this.claz = claz;
      this.beamCoder = beamCoder;
      this.child = new BoundReference(0, new ObjectType(claz), true);
    }

    @Override public Expression child() {
      return child;
    }

    @Override public ExprCode doGenCode(CodegenContext ctx, ExprCode ev) {
      // Code to serialize.
      ExprCode input = child.genCode(ctx);
      String javaType = CodeGenerator.javaType(dataType());
      String outputStream = "ByteArrayOutputStream baos = new ByteArrayOutputStream();";

      String serialize = outputStream + "$beamCoder.encode(${input.value}, baos); baos.toByteArray();";

      String outside = "final $javaType output = ${input.isNull} ? ${CodeGenerator.defaultValue(dataType)} : $serialize;";

      List<String> instructions = new ArrayList<>();
      instructions.add(outside);

      Seq<String> parts = JavaConversions.collectionAsScalaIterable(instructions).toSeq();
      StringContext stringContext = new StringContext(parts);
      Block.BlockHelper blockHelper = new Block.BlockHelper(stringContext);
      List<Object> args = new ArrayList<>();
      args.add(new VariableValue("beamCoder", Coder.class));
      args.add(new SimpleExprValue("input.value", ExprValue.class));
      args.add(new VariableValue("javaType", String.class));
      args.add(new SimpleExprValue("input.isNull", Boolean.class));
      args.add(new SimpleExprValue("CodeGenerator.defaultValue(dataType)", String.class));
      args.add(new VariableValue("$serialize", String.class));
      Block code = blockHelper.code(JavaConversions.collectionAsScalaIterable(args).toSeq());

      return ev.copy(input.code().$plus(code), input.isNull(), new VariableValue("output", Array.class));
    }

    @Override public DataType dataType() {
      return BinaryType;
    }

    @Override public Object productElement(int n) {
      if (n == 0) {
        return this;
      } else {
        throw new IndexOutOfBoundsException(String.valueOf(n));
      }
    }

    @Override public int productArity() {
      //TODO test with spark Encoders if the arity of 1 is ok
      return 1;
    }

    @Override public boolean canEqual(Object that) {
      return (that instanceof EncodeUsingBeamCoder);
    }

    @Override public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      EncodeUsingBeamCoder<?> that = (EncodeUsingBeamCoder<?>) o;
      return claz.equals(that.claz) && beamCoder.equals(that.beamCoder);
    }

    @Override public int hashCode() {
      return Objects.hash(super.hashCode(), claz, beamCoder);
    }
  }

  /*case class EncodeUsingSerializer(child: Expression, kryo: Boolean)
      extends UnaryExpression with NonSQLExpression with SerializerSupport {

    override def nullSafeEval(input: Any): Any = {
        serializerInstance.serialize(input).array()
    }

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
        val serializer = addImmutableScodererializerIfNeeded(ctx)
        // Code to serialize.
        val input = child.genCode(ctx)
        val javaType = CodeGenerator.javaType(dataType)
        val serialize = s"$serializer.serialize(${input.value}, null).array()"

        val code = input.code + code"""
    final $javaType ${ev.value} =
    ${input.isNull} ? ${CodeGenerator.defaultValue(dataType)} : $serialize;
    """
    ev.copy(code = code, isNull = input.isNull)
  }

    override def dataType: DataType = BinaryType
  }*/

  private static class DecodeUsingBeamCoder<T> extends UnaryExpression implements  NonSQLExpression{

    private ClassTag<T> classTag;
    private Coder<T> beamCoder;

    private DecodeUsingBeamCoder(ClassTag<T> classTag, Coder<T> beamCoder) {
      this.classTag = classTag;
      this.beamCoder = beamCoder;
    }

    @Override public Expression child() {
      return new Cast(new GetColumnByOrdinal(0, BinaryType), BinaryType);
    }

    @Override public ExprCode doGenCode(CodegenContext ctx, ExprCode ev) {
      return null;
    }

    @Override public DataType dataType() {
      return new ObjectType(classTag.runtimeClass());
    }

    @Override public Object productElement(int n) {
      if (n == 0) {
        return this;
      } else {
        throw new IndexOutOfBoundsException(String.valueOf(n));
      }
    }

    @Override public int productArity() {
      //TODO test with spark Encoders if the arity of 1 is ok
      return 1;
    }

    @Override public boolean canEqual(Object that) {
      return (that instanceof DecodeUsingBeamCoder);
    }

    @Override public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DecodeUsingBeamCoder<?> that = (DecodeUsingBeamCoder<?>) o;
      return classTag.equals(that.classTag) && beamCoder.equals(that.beamCoder);
    }

    @Override public int hashCode() {
      return Objects.hash(super.hashCode(), classTag, beamCoder);
    }
  }
/*
case class DecodeUsingSerializer[T](child: Expression, tag: ClassTag[T], kryo: Boolean)
      extends UnaryExpression with NonSQLExpression with SerializerSupport {

    override def nullSafeEval(input: Any): Any = {
        val inputBytes = java.nio.ByteBuffer.wrap(input.asInstanceOf[Array[Byte]])
        serializerInstance.deserialize(inputBytes)
    }

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
        val serializer = addImmutableSerializerIfNeeded(ctx)
        // Code to deserialize.
        val input = child.genCode(ctx)
        val javaType = CodeGenerator.javaType(dataType)
        val deserialize =
        s"($javaType) $serializer.deserialize(java.nio.ByteBuffer.wrap(${input.value}), null)"

        val code = input.code + code"""
    final $javaType ${ev.value} =
    ${input.isNull} ? ${CodeGenerator.defaultValue(dataType)} : $deserialize;
    """
    ev.copy(code = code, isNull = input.isNull)
  }

    override def dataType: DataType = ObjectType(tag.runtimeClass)
  }
*/

}
