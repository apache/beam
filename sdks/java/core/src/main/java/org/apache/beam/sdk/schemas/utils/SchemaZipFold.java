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
package org.apache.beam.sdk.schemas.utils;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

/**
 * Visitor that zips schemas, and accepts pairs of fields and their types.
 *
 * <p>Values returned by `accept` are accumulated.
 */
@Experimental(Kind.SCHEMAS)
public abstract class SchemaZipFold<T> implements Serializable {

  public final T apply(Schema left, Schema right) {
    return visit(this, Context.EMPTY, FieldType.row(left), FieldType.row(right));
  }

  /** Accumulate two results together. */
  public abstract T accumulate(T left, T right);

  /** Accepts two components, context.parent() is always ROW, MAP, ARRAY or absent. */
  public abstract T accept(Context context, FieldType left, FieldType right);

  /** Accepts two fields, context.parent() is always ROW. */
  public abstract T accept(Context context, Optional<Field> left, Optional<Field> right);

  /** Context referring to a current position in a schema. */
  @AutoValue
  public abstract static class Context {
    /** Field path from a root of a schema. */
    public abstract List<String> path();

    /** Type of parent node in a tree. */
    public abstract Optional<TypeName> parent();

    public static final Context EMPTY = Context.create(Collections.emptyList(), Optional.empty());

    public Context withPathPart(String part) {
      return create(ImmutableList.<String>builder().addAll(path()).add(part).build(), parent());
    }

    public Context withParent(TypeName parent) {
      return create(path(), Optional.of(parent));
    }

    public static Context create(List<String> path, Optional<TypeName> parent) {
      return new AutoValue_SchemaZipFold_Context(path, parent);
    }
  }

  static <T> T visit(SchemaZipFold<T> zipFold, Context context, FieldType left, FieldType right) {
    if (left.getTypeName() != right.getTypeName()) {
      return zipFold.accept(context, left, right);
    }

    Context newContext = context.withParent(left.getTypeName());

    switch (left.getTypeName()) {
      case ARRAY:
      case ITERABLE:
        return zipFold.accumulate(
            zipFold.accept(context, left, right),
            visit(
                zipFold,
                newContext,
                left.getCollectionElementType(),
                right.getCollectionElementType()));

      case ROW:
        return visitRow(zipFold, newContext, left.getRowSchema(), right.getRowSchema());

      case MAP:
        return zipFold.accumulate(
            zipFold.accept(context, left, right),
            visit(
                zipFold,
                newContext,
                left.getCollectionElementType(),
                right.getCollectionElementType()));

      default:
        return zipFold.accept(context, left, right);
    }
  }

  static <T> T visitRow(SchemaZipFold<T> zipFold, Context context, Schema left, Schema right) {
    T node = zipFold.accept(context, FieldType.row(left), FieldType.row(right));

    Stream<String> union =
        Stream.concat(
                left.getFields().stream().map(Schema.Field::getName),
                right.getFields().stream().map(Schema.Field::getName))
            .distinct();

    Stream<String> intersection =
        left.getFields().stream().map(Schema.Field::getName).filter(right::hasField);

    T inner0 =
        intersection
            .map(
                name ->
                    visit(
                        zipFold,
                        context.withPathPart(name).withParent(TypeName.ROW),
                        left.getField(name).getType(),
                        right.getField(name).getType()))
            .reduce(node, zipFold::accumulate);

    T inner1 =
        union
            .map(
                name -> {
                  Optional<Field> field0 = Optional.empty();
                  Optional<Field> field1 = Optional.empty();

                  if (left.hasField(name)) {
                    field0 = Optional.of(left.getField(name));
                  }

                  if (right.hasField(name)) {
                    field1 = Optional.of(right.getField(name));
                  }

                  Context newContext = context.withPathPart(name).withParent(TypeName.ROW);
                  return zipFold.accept(newContext, field0, field1);
                })
            .reduce(node, zipFold::accumulate);

    return zipFold.accumulate(zipFold.accumulate(node, inner0), inner1);
  }
}
