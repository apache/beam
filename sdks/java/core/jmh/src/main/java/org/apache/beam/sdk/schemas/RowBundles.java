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
package org.apache.beam.sdk.schemas;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.joda.time.DateTime;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

public interface RowBundles {
  @State(Scope.Benchmark)
  class IntBundle extends RowBundle {
    public IntBundle() {
      super(Field.class);
    }

    @DefaultSchema(JavaFieldSchema.class)
    public static class Field {
      public int field;
    }
  }

  @State(Scope.Benchmark)
  class NestedIntBundle extends RowBundle {
    public NestedIntBundle() {
      super(Field.class);
    }

    @DefaultSchema(JavaFieldSchema.class)
    public static class Field {
      public IntBundle.Field field;
    }
  }

  @State(Scope.Benchmark)
  class StringBundle extends RowBundle {
    public StringBundle() {
      super(Field.class);
    }

    @DefaultSchema(JavaFieldSchema.class)
    public static class Field {
      public String field;
    }
  }

  @State(Scope.Benchmark)
  class StringBuilderBundle extends RowBundle {
    public StringBuilderBundle() {
      super(Field.class);
    }

    @DefaultSchema(JavaFieldSchema.class)
    public static class Field {
      public StringBuilder field;
    }
  }

  @State(Scope.Benchmark)
  class DateTimeBundle extends RowBundle {
    public DateTimeBundle() {
      super(Field.class);
    }

    @DefaultSchema(JavaFieldSchema.class)
    public static class Field {
      public DateTime field;
    }
  }

  @State(Scope.Benchmark)
  class BytesBundle extends RowBundle {
    public BytesBundle() {
      super(Field.class);
    }

    @DefaultSchema(JavaFieldSchema.class)
    public static class Field {
      public byte[] field;
    }
  }

  @State(Scope.Benchmark)
  class NestedBytesBundle extends RowBundle {
    public NestedBytesBundle() {
      super(Field.class);
    }

    @DefaultSchema(JavaFieldSchema.class)
    public static class Field {
      public BytesBundle.Field field;
    }
  }

  @State(Scope.Benchmark)
  class ByteBufferBundle extends RowBundle {
    public ByteBufferBundle() {
      super(Field.class);
    }

    @DefaultSchema(JavaFieldSchema.class)
    public static class Field {
      public ByteBuffer field;
    }
  }

  @State(Scope.Benchmark)
  class ArrayOfNestedIntBundle extends RowBundle {
    public ArrayOfNestedIntBundle() {
      super(Field.class);
    }

    @DefaultSchema(JavaFieldSchema.class)
    public static class Field {
      public IntBundle.Field[] field;
    }
  }

  @State(Scope.Benchmark)
  class MapOfIntBundle extends RowBundle {
    public MapOfIntBundle() {
      super(Field.class);
    }

    @DefaultSchema(JavaFieldSchema.class)
    public static class Field {
      public Map<Integer, Integer> field;
    }
  }

  @State(Scope.Benchmark)
  class MapOfNestedIntBundle extends RowBundle {
    public MapOfNestedIntBundle() {
      super(Field.class);
    }

    @DefaultSchema(JavaFieldSchema.class)
    public static class Field {
      public Map<Integer, NestedIntBundle.Field> field;
    }
  }
}
