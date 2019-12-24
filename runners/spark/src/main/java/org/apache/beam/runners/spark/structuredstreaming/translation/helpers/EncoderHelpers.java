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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.beam.runners.spark.structuredstreaming.translation.SchemaHelpers;
import org.apache.beam.sdk.coders.Coder;
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
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.ObjectType;
import scala.StringContext;
import scala.collection.JavaConversions;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

/** {@link Encoders} utility class. */
public class EncoderHelpers {
  /**
   * Wrap a Beam coder into a Spark Encoder using Catalyst Expression Encoders (which uses java code
   * generation).
   */
  public static <T> Encoder<T> fromBeamCoder(Coder<T> coder) {
    Class<? super T> clazz = coder.getEncodedTypeDescriptor().getRawType();
    ClassTag<T> classTag = ClassTag$.MODULE$.apply(clazz);
    List<Expression> serializers =
        Collections.singletonList(
            new EncodeUsingBeamCoder<>(new BoundReference(0, new ObjectType(clazz), true), coder));

    return new ExpressionEncoder<>(
        SchemaHelpers.binarySchema(),
        false,
        JavaConversions.collectionAsScalaIterable(serializers).toSeq(),
        new DecodeUsingBeamCoder<>(
            new Cast(new GetColumnByOrdinal(0, BinaryType), BinaryType), classTag, coder),
        classTag);
  }

  /**
   * Catalyst Expression that serializes elements using Beam {@link Coder}.
   *
   * @param <T>: Type of elements ot be serialized.
   */
  public static class EncodeUsingBeamCoder<T> extends UnaryExpression
      implements NonSQLExpression, Serializable {

    private final Expression child;
    private final Coder<T> coder;

    public EncodeUsingBeamCoder(Expression child, Coder<T> coder) {
      this.child = child;
      this.coder = coder;
    }

    @Override
    public Expression child() {
      return child;
    }

    @Override
    public ExprCode doGenCode(CodegenContext ctx, ExprCode ev) {
      String accessCode = ctx.addReferenceObj("coder", coder, coder.getClass().getName());
      ExprCode input = child.genCode(ctx);
      String javaType = CodeGenerator.javaType(dataType());

      List<String> parts = new ArrayList<>();
      List<Object> args = new ArrayList<>();
      /*
            CODE GENERATED
            final ${javaType} ${ev.value} = (${input.isNull})
                ? null
                : org.apache.beam.runners.spark.structuredstreaming.translation.helpers.CoderHelpers.toByteArray(${input.value}, ${coder});
      */
      parts.add("final ");
      args.add(javaType);
      parts.add(" ");
      args.add(ev.value());
      parts.add(" = (");
      args.add(input.isNull());
      parts.add(
          ") ? null : org.apache.beam.runners.spark.structuredstreaming.translation.helpers.CoderHelpers.toByteArray(");
      args.add(input.value());
      parts.add(", ");
      args.add(accessCode);
      parts.add(");");

      StringContext sc =
          new StringContext(JavaConversions.collectionAsScalaIterable(parts).toSeq());
      Block code =
          (new Block.BlockHelper(sc)).code(JavaConversions.collectionAsScalaIterable(args).toSeq());

      return ev.copy(input.code().$plus(code), input.isNull(), ev.value());
    }

    @Override
    public DataType dataType() {
      return BinaryType;
    }

    @Override
    public Object productElement(int n) {
      switch (n) {
        case 0:
          return child;
        case 1:
          return coder;
        default:
          throw new ArrayIndexOutOfBoundsException("productElement out of bounds");
      }
    }

    @Override
    public int productArity() {
      return 2;
    }

    @Override
    public boolean canEqual(Object that) {
      return (that instanceof EncodeUsingBeamCoder);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      EncodeUsingBeamCoder<?> that = (EncodeUsingBeamCoder<?>) o;
      return child.equals(that.child) && coder.equals(that.coder);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), child, coder);
    }
  }

  /**
   * Catalyst Expression that deserializes elements using Beam {@link Coder}.
   *
   * @param <T>: Type of elements ot be serialized.
   */
  public static class DecodeUsingBeamCoder<T> extends UnaryExpression
      implements NonSQLExpression, Serializable {

    private final Expression child;
    private final ClassTag<T> classTag;
    private final Coder<T> coder;

    public DecodeUsingBeamCoder(Expression child, ClassTag<T> classTag, Coder<T> coder) {
      this.child = child;
      this.classTag = classTag;
      this.coder = coder;
    }

    @Override
    public Expression child() {
      return child;
    }

    @Override
    public ExprCode doGenCode(CodegenContext ctx, ExprCode ev) {
      String accessCode = ctx.addReferenceObj("coder", coder, coder.getClass().getName());
      ExprCode input = child.genCode(ctx);
      String javaType = CodeGenerator.javaType(dataType());

      List<String> parts = new ArrayList<>();
      List<Object> args = new ArrayList<>();
      /*
            CODE GENERATED:
            final ${javaType} ${ev.value} = (${input.isNull})
                ? null
                : (${javaType}) org.apache.beam.runners.spark.structuredstreaming.translation.helpers.CoderHelpers.fromByteArray(${input.value}, ${coder});
      */
      parts.add("final ");
      args.add(javaType);
      parts.add(" ");
      args.add(ev.value());
      parts.add(" = (");
      args.add(input.isNull());
      parts.add(") ? null : (");
      args.add(javaType);
      parts.add(
          ") org.apache.beam.runners.spark.structuredstreaming.translation.helpers.CoderHelpers.fromByteArray(");
      args.add(input.value());
      parts.add(", ");
      args.add(accessCode);
      parts.add(");");

      StringContext sc =
          new StringContext(JavaConversions.collectionAsScalaIterable(parts).toSeq());
      Block code =
          (new Block.BlockHelper(sc)).code(JavaConversions.collectionAsScalaIterable(args).toSeq());
      return ev.copy(input.code().$plus(code), input.isNull(), ev.value());
    }

    @Override
    public DataType dataType() {
      return new ObjectType(classTag.runtimeClass());
    }

    @Override
    public Object productElement(int n) {
      switch (n) {
        case 0:
          return child;
        case 1:
          return classTag;
        case 2:
          return coder;
        default:
          throw new ArrayIndexOutOfBoundsException("productElement out of bounds");
      }
    }

    @Override
    public int productArity() {
      return 3;
    }

    @Override
    public boolean canEqual(Object that) {
      return (that instanceof DecodeUsingBeamCoder);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DecodeUsingBeamCoder<?> that = (DecodeUsingBeamCoder<?>) o;
      return child.equals(that.child) && classTag.equals(that.classTag) && coder.equals(that.coder);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), child, classTag, coder);
    }
  }
}
