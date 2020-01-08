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

  /*
   --------- Bridges from Beam Coders to Spark Encoders
  */

  /**
   * Wrap a Beam coder into a Spark Encoder using Catalyst Expression Encoders (which uses java code
   * generation).
   */
  public static <T> Encoder<T> fromBeamCoder(Coder<T> beamCoder) {

    List<Expression> serialiserList = new ArrayList<>();
    Class<? super T> claz = beamCoder.getEncodedTypeDescriptor().getRawType();
    BeamCoderWrapper<T> beamCoderWrapper = new BeamCoderWrapper<>(beamCoder);

    serialiserList.add(
        new EncodeUsingBeamCoder<>(
            new BoundReference(0, new ObjectType(claz), true), beamCoderWrapper));
    ClassTag<T> classTag = ClassTag$.MODULE$.apply(claz);
    return new ExpressionEncoder<>(
        SchemaHelpers.binarySchema(),
        false,
        JavaConversions.collectionAsScalaIterable(serialiserList).toSeq(),
        new DecodeUsingBeamCoder<>(
            new Cast(new GetColumnByOrdinal(0, BinaryType), BinaryType),
            classTag,
            beamCoderWrapper),
        classTag);
  }

  public static class BeamCoderWrapper<T> implements Serializable {

    private Coder<T> beamCoder;

    public BeamCoderWrapper(Coder<T> beamCoder) {
      this.beamCoder = beamCoder;
    }

    public byte[] encode(boolean isInputNull, T inputValue) {
      if (isInputNull) {
        return null;
      } else {
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        try {
          beamCoder.encode(inputValue, baos);
        } catch (Exception e) {
          throw org.apache.beam.sdk.util.UserCodeException.wrap(e);
        }
        return baos.toByteArray();
      }
    }

    public T decode(boolean isInputNull, byte[] inputValue) {
      try {
        return isInputNull ? null : beamCoder.decode(new java.io.ByteArrayInputStream(inputValue));
      } catch (Exception e) {
        throw org.apache.beam.sdk.util.UserCodeException.wrap(e);
      }
    }
  }

  /**
   * Catalyst Expression that serializes elements using Beam {@link Coder}.
   *
   * @param <T>: Type of elements ot be serialized.
   */
  public static class EncodeUsingBeamCoder<T> extends UnaryExpression
      implements NonSQLExpression, Serializable {

    private Expression child;
    private BeamCoderWrapper<T> beamCoderWrapper;

    public EncodeUsingBeamCoder(Expression child, BeamCoderWrapper<T> beamCoderWrapper) {
      this.child = child;
      this.beamCoderWrapper = beamCoderWrapper;
    }

    @Override
    public Expression child() {
      return child;
    }

    @Override
    public ExprCode doGenCode(CodegenContext ctx, ExprCode ev) {
      String accessCode =
          ctx.addReferenceObj(
              "beamCoderWrapper", beamCoderWrapper, BeamCoderWrapper.class.getName());
      ExprCode input = child.genCode(ctx);
      String javaType = CodeGenerator.javaType(dataType());

      List<String> parts = new ArrayList<>();
      List<Object> args = new ArrayList<>();
      /*
            CODE GENERATED
            final $javaType ${ev.value} = $beamCoderWrapper.encode(${input.isNull}, ${input.value});
      */
      parts.add("final ");
      args.add(javaType);
      parts.add(" ");
      args.add(ev.value());
      parts.add(" = ");
      args.add(accessCode);
      parts.add(".encode(");
      args.add(input.isNull());
      parts.add(", ");
      args.add(input.value());
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
          return beamCoderWrapper;
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
      return beamCoderWrapper.equals(that.beamCoderWrapper) && child.equals(that.child);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), child, beamCoderWrapper);
    }
  }

  /**
   * Catalyst Expression that deserializes elements using Beam {@link Coder}.
   *
   * @param <T>: Type of elements ot be serialized.
   */
  public static class DecodeUsingBeamCoder<T> extends UnaryExpression
      implements NonSQLExpression, Serializable {

    private Expression child;
    private ClassTag<T> classTag;
    private BeamCoderWrapper<T> beamCoderWrapper;

    public DecodeUsingBeamCoder(
        Expression child, ClassTag<T> classTag, BeamCoderWrapper<T> beamCoderWrapper) {
      this.child = child;
      this.classTag = classTag;
      this.beamCoderWrapper = beamCoderWrapper;
    }

    @Override
    public Expression child() {
      return child;
    }

    @Override
    public ExprCode doGenCode(CodegenContext ctx, ExprCode ev) {
      String accessCode =
          ctx.addReferenceObj(
              "beamCoderWrapper", beamCoderWrapper, BeamCoderWrapper.class.getName());
      ExprCode input = child.genCode(ctx);
      String javaType = CodeGenerator.javaType(dataType());
      List<String> parts = new ArrayList<>();
      List<Object> args = new ArrayList<>();
      /*
            CODE GENERATED:
            final $javaType ${ev.value} = ($javaType) $beamCoderWrapper.decode(${input.isNull}, ${input.value});
      */
      parts.add("final ");
      args.add(javaType);
      parts.add(" ");
      args.add(ev.value());
      parts.add(" = (");
      args.add(javaType);
      parts.add(") ");
      args.add(accessCode);
      parts.add(".decode(");
      args.add(input.isNull());
      parts.add(", ");
      args.add(input.value());
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
          return beamCoderWrapper;
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
      return child.equals(that.child)
          && classTag.equals(that.classTag)
          && beamCoderWrapper.equals(that.beamCoderWrapper);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), child, classTag, beamCoderWrapper);
    }
  }
}
