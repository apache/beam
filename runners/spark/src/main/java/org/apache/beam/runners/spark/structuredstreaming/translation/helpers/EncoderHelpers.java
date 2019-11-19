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

import java.io.ByteArrayInputStream;
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
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.ObjectType;
import scala.StringContext;
import scala.Tuple2;
import scala.collection.JavaConversions;
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
  public static <T> Encoder<T> fromBeamCoder(Coder<T> beamCoder/*, Class<T> claz*/){

    List<Expression> serialiserList = new ArrayList<>();
    Class<T> claz = (Class<T>) Object.class;
    serialiserList.add(new EncodeUsingBeamCoder<>(new BoundReference(0, new ObjectType(claz), true), beamCoder));
    ClassTag<T> classTag = ClassTag$.MODULE$.apply(claz);
    return new ExpressionEncoder<>(
        SchemaHelpers.binarySchema(),
        false,
        JavaConversions.collectionAsScalaIterable(serialiserList).toSeq(),
        new DecodeUsingBeamCoder<>(new Cast(new GetColumnByOrdinal(0, BinaryType), BinaryType), classTag, beamCoder),
        classTag);
  }

  public static class EncodeUsingBeamCoder<T> extends UnaryExpression implements NonSQLExpression {

    private Expression child;
    private Coder<T> beamCoder;

    public EncodeUsingBeamCoder(Expression child, Coder<T> beamCoder) {
      this.child = child;
      this.beamCoder = beamCoder;
    }

    @Override public Expression child() {
      return child;
    }

    @Override public ExprCode doGenCode(CodegenContext ctx, ExprCode ev) {
      // Code to serialize.
      String accessCode = ctx.addReferenceObj("beamCoder", beamCoder, beamCoder.getClass().getName());
      ExprCode input = child.genCode(ctx);

      /*
        CODE GENERATED
        byte[] ${ev.value};
       try {
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        if ({input.isNull})
            ${ev.value} = null;
        else{
            $beamCoder.encode(${input.value}, baos);
            ${ev.value} =  baos.toByteArray();
        }
        } catch (Exception e) {
          throw new RuntimeException(org.apache.beam.sdk.util.UserCodeException.wrap(e));
        }
      */
      List<String> parts = new ArrayList<>();
      parts.add("byte[] ");
      parts.add(";try { java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream(); if (");
      parts.add(") ");
      parts.add(" = null; else{");
      parts.add(".encode(");
      parts.add(", baos); ");
      parts.add(" = baos.toByteArray();}} catch (Exception e) {throw new RuntimeException(org.apache.beam.sdk.util.UserCodeException.wrap(e));}");

      StringContext sc = new StringContext(JavaConversions.collectionAsScalaIterable(parts).toSeq());

      List<Object> args = new ArrayList<>();

      args.add(ev.value());
      args.add(input.isNull());
      args.add(ev.value());
      args.add(accessCode);
      args.add(input.value());
      args.add(ev.value());
      Block code = (new Block.BlockHelper(sc))
          .code(JavaConversions.collectionAsScalaIterable(args).toSeq());

      return ev.copy(input.code().$plus(code), input.isNull(),ev.value());
    }


    @Override public DataType dataType() {
      return BinaryType;
    }

    @Override public Object productElement(int n) {
      switch (n) {
        case 0:
          return child;
        case 1:
          return beamCoder;
        default:
          throw new ArrayIndexOutOfBoundsException("productElement out of bounds");
      }
    }

    @Override public int productArity() {
      return 2;
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
      return beamCoder.equals(that.beamCoder);
    }

    @Override public int hashCode() {
      return Objects.hash(super.hashCode(), beamCoder);
    }
  }

  public static class DecodeUsingBeamCoder<T> extends UnaryExpression implements  NonSQLExpression{

    private Expression child;
    private ClassTag<T> classTag;
    private Coder<T> beamCoder;

    public DecodeUsingBeamCoder(Expression child, ClassTag<T> classTag, Coder<T> beamCoder) {
      this.child = child;
      this.classTag = classTag;
      this.beamCoder = beamCoder;
    }

    @Override public Expression child() {
      return child;
    }

    @Override public ExprCode doGenCode(CodegenContext ctx, ExprCode ev) {
      // Code to deserialize.
      String accessCode = ctx.addReferenceObj("beamCoder", beamCoder, beamCoder.getClass().getName());
      ExprCode input = child.genCode(ctx);
      String javaType = CodeGenerator.javaType(dataType());

/*
     CODE GENERATED:
     try {
      final $javaType ${ev.value} =
      ${input.isNull} ?
      ${CodeGenerator.defaultValue(dataType)} :
      ($javaType) $beamCoder.decode(new java.io.ByteArrayInputStream(${input.value}));
     } catch (IOException e) {
      throw new RuntimeException(org.apache.beam.sdk.util.UserCodeException.wrap(e));
     }
*/

      List<String> parts = new ArrayList<>();
      parts.add("try { final ");
      parts.add(" ");
      parts.add(" =");
      parts.add("?");
      parts.add(":");
      parts.add("(");
      parts.add(") ");
      parts.add(".decode(new java.io.ByteArrayInputStream(");
      parts.add("));  } catch (IOException e) {throw new RuntimeException(org.apache.beam.sdk.util.UserCodeException.wrap(e));}");

      StringContext sc = new StringContext(JavaConversions.collectionAsScalaIterable(parts).toSeq());

      List<Object> args = new ArrayList<>();
      args.add(javaType);
      args.add(ev.value());
      args.add(input.isNull());
      args.add(CodeGenerator.defaultValue(dataType(), false));
      args.add(javaType);
      args.add(accessCode);
      args.add(input.value());
      Block code = (new Block.BlockHelper(sc))
          .code(JavaConversions.collectionAsScalaIterable(args).toSeq());

      return ev.copy(input.code().$plus(code), input.isNull(), ev.value());

    }

    @Override public Object nullSafeEval(Object input) {
      try {
        return beamCoder.decode(new ByteArrayInputStream((byte[]) input));
      } catch (Exception e) {
        throw new IllegalStateException("Error decoding bytes for coder: " + beamCoder, e);
      }
    }

    @Override public DataType dataType() {
      return new ObjectType(classTag.runtimeClass());
    }

    @Override public Object productElement(int n) {
      switch (n) {
        case 0:
          return child;
        case 1:
          return classTag;
        case 2:
          return beamCoder;
        default:
          throw new ArrayIndexOutOfBoundsException("productElement out of bounds");
      }
    }

    @Override public int productArity() {
      return 3;
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
}
