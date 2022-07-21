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
package org.apache.beam.sdk.jmh.schemas;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

public interface RowBundles {
  @State(Scope.Benchmark)
  class IntBundle extends RowBundle<IntBundle.Field> {
    public IntBundle() {
      super(Field.class);
    }

    @DefaultSchema(JavaFieldSchema.class)
    public static class Field {
      public int field;
    }
  }

  @State(Scope.Benchmark)
  class NestedIntBundle extends RowBundle<NestedIntBundle.Field> {
    public NestedIntBundle() {
      super(Field.class);
    }

    @DefaultSchema(JavaFieldSchema.class)
    public static class Field {
      public IntBundle.Field field;
    }

    @Override
    protected final void readField(Row row, Blackhole bh) {
      bh.consume(row.getRow(0).getValue(0));
    }
  }

  @State(Scope.Benchmark)
  class StringBundle extends RowBundle<StringBundle.Field> {
    public StringBundle() {
      super(Field.class);
    }

    @DefaultSchema(JavaFieldSchema.class)
    public static class Field {
      public String field;
    }
  }

  @State(Scope.Benchmark)
  class StringBuilderBundle extends RowBundle<StringBuilderBundle.Field> {
    public StringBuilderBundle() {
      super(Field.class);
    }

    @DefaultSchema(JavaFieldSchema.class)
    public static class Field {
      public StringBuilder field;
    }
  }

  @State(Scope.Benchmark)
  class DateTimeBundle extends RowBundle<DateTimeBundle.Field> {
    public DateTimeBundle() {
      super(Field.class);
    }

    @DefaultSchema(JavaFieldSchema.class)
    public static class Field {
      public DateTime field;
    }
  }

  @State(Scope.Benchmark)
  class BytesBundle extends RowBundle<BytesBundle.Field> {
    public BytesBundle() {
      super(Field.class);
    }

    @DefaultSchema(JavaFieldSchema.class)
    public static class Field {
      public byte[] field;
    }
  }

  @State(Scope.Benchmark)
  class NestedBytesBundle extends RowBundle<NestedBytesBundle.Field> {
    public NestedBytesBundle() {
      super(Field.class);
    }

    @DefaultSchema(JavaFieldSchema.class)
    public static class Field {
      public BytesBundle.Field field;
    }

    @Override
    protected final void readField(Row row, Blackhole bh) {
      bh.consume(row.getRow(0).getValue(0));
    }
  }

  @State(Scope.Benchmark)
  class ByteBufferBundle extends RowBundle<ByteBufferBundle.Field> {
    public ByteBufferBundle() {
      super(Field.class);
    }

    @DefaultSchema(JavaFieldSchema.class)
    public static class Field {
      public ByteBuffer field;
    }
  }

  @State(Scope.Benchmark)
  class ArrayOfStringBundle extends RowBundle<ArrayOfStringBundle.Field> {
    public ArrayOfStringBundle() {
      super(Field.class);
    }

    @DefaultSchema(JavaFieldSchema.class)
    public static class Field {
      public String[] field;
    }

    @Override
    protected final void readField(Row row, Blackhole bh) {
      bh.consume(((List<String>) row.getValue(0)).get(0));
    }
  }

  @State(Scope.Benchmark)
  class ArrayOfNestedStringBundle extends RowBundle<ArrayOfNestedStringBundle.Field> {
    public ArrayOfNestedStringBundle() {
      super(Field.class);
    }

    @DefaultSchema(JavaFieldSchema.class)
    public static class Field {
      public StringBundle.Field[] field;
    }

    @Override
    protected final void readField(Row row, Blackhole bh) {
      bh.consume(((List<Row>) row.getValue(0)).get(0).getValue(0));
    }
  }

  @State(Scope.Benchmark)
  class MapOfIntBundle extends RowBundle<MapOfIntBundle.Field> {
    public MapOfIntBundle() {
      super(Field.class);
    }

    @DefaultSchema(JavaFieldSchema.class)
    public static class Field {
      public Map<Integer, Integer> field;
    }

    @Override
    protected final void readField(Row row, Blackhole bh) {
      Map.Entry<?, ?> entry = row.getMap(0).entrySet().iterator().next();
      bh.consume(entry.getKey());
      bh.consume(entry.getValue());
    }
  }

  @State(Scope.Benchmark)
  class MapOfNestedIntBundle extends RowBundle<MapOfNestedIntBundle.Field> {
    public MapOfNestedIntBundle() {
      super(Field.class);
    }

    @DefaultSchema(JavaFieldSchema.class)
    public static class Field {
      public Map<Integer, NestedIntBundle.Field> field;
    }

    @Override
    protected final void readField(Row row, Blackhole bh) {
      Map.Entry<Integer, Row> entry = row.<Integer, Row>getMap(0).entrySet().iterator().next();
      bh.consume(entry.getKey());
      bh.consume(entry.getValue().getValue(0));
    }
  }
}
