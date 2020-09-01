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
package org.apache.beam.sdk.extensions.avro.schemas.utils;

import com.pholser.junit.quickcheck.generator.GenerationStatus;
import com.pholser.junit.quickcheck.generator.Generator;
import com.pholser.junit.quickcheck.random.SourceOfRandomness;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ObjectArrays;

/** QuickCheck generators for AVRO. */
class AvroGenerators {

  /** Generates arbitrary AVRO schemas. */
  public static class SchemaGenerator extends BaseSchemaGenerator {

    public static final SchemaGenerator INSTANCE = new SchemaGenerator();

    private static final ImmutableList<Schema.Type> PRIMITIVE_TYPES =
        ImmutableList.of(
            Schema.Type.STRING,
            Schema.Type.BYTES,
            Schema.Type.INT,
            Schema.Type.LONG,
            Schema.Type.FLOAT,
            Schema.Type.DOUBLE,
            Schema.Type.BOOLEAN);

    private static final ImmutableList<Schema.Type> ALL_TYPES =
        ImmutableList.<Schema.Type>builder()
            .addAll(PRIMITIVE_TYPES)
            .add(Schema.Type.FIXED)
            .add(Schema.Type.ENUM)
            .add(Schema.Type.RECORD)
            .add(Schema.Type.ARRAY)
            .add(Schema.Type.MAP)
            .add(Schema.Type.UNION)
            .add(Schema.Type.ARRAY)
            .build();

    private static final int MAX_NESTING = 10;

    @Override
    public Schema generate(SourceOfRandomness random, GenerationStatus status) {
      Schema.Type type;

      if (nesting(status) >= MAX_NESTING) {
        type = random.choose(PRIMITIVE_TYPES);
      } else {
        type = random.choose(ALL_TYPES);
      }

      if (PRIMITIVE_TYPES.contains(type)) {
        return Schema.create(type);
      } else {
        nestingInc(status);

        if (type == Schema.Type.FIXED) {
          int size = random.choose(Arrays.asList(1, 5, 12));
          return Schema.createFixed("fixed_" + branch(status), "", "", size);
        } else if (type == Schema.Type.UNION) {
          // only nullable fields, everything else isn't supported in row conversion code
          return UnionSchemaGenerator.INSTANCE.generate(random, status);
        } else if (type == Schema.Type.ENUM) {
          return EnumSchemaGenerator.INSTANCE.generate(random, status);
        } else if (type == Schema.Type.RECORD) {
          return RecordSchemaGenerator.INSTANCE.generate(random, status);
        } else if (type == Schema.Type.MAP) {
          return Schema.createMap(generate(random, status));
        } else if (type == Schema.Type.ARRAY) {
          return Schema.createArray(generate(random, status));
        } else {
          throw new AssertionError("Unexpected AVRO type: " + type);
        }
      }
    }
  }

  public static class RecordSchemaGenerator extends BaseSchemaGenerator {

    public static final RecordSchemaGenerator INSTANCE = new RecordSchemaGenerator();

    @Override
    public Schema generate(SourceOfRandomness random, GenerationStatus status) {
      List<Schema.Field> fields =
          IntStream.range(0, random.nextInt(0, status.size()) + 1)
              .mapToObj(
                  i -> {
                    // deterministically avoid collisions in record names
                    branchPush(status, String.valueOf(i));
                    Schema.Field field =
                        createField(i, SchemaGenerator.INSTANCE.generate(random, status));
                    branchPop(status);
                    return field;
                  })
              .collect(Collectors.toList());

      return Schema.createRecord("record_" + branch(status), "", "example", false, fields);
    }

    private Schema.Field createField(int i, Schema schema) {
      return new Schema.Field("field_" + i, schema, null, (Object) null);
    }
  }

  static class UnionSchemaGenerator extends BaseSchemaGenerator {

    public static final UnionSchemaGenerator INSTANCE = new UnionSchemaGenerator();

    @Override
    public Schema generate(SourceOfRandomness random, GenerationStatus status) {
      Map<String, Schema> schemaMap =
          IntStream.range(0, random.nextInt(0, status.size()) + 1)
              .mapToObj(
                  i -> {
                    // deterministically avoid collisions in record names
                    branchPush(status, String.valueOf(i));
                    Schema schema =
                        SchemaGenerator.INSTANCE
                            // nested unions aren't supported in AVRO
                            .filter(x -> x.getType() != Schema.Type.UNION)
                            .generate(random, status);
                    branchPop(status);
                    return schema;
                  })
              // AVRO requires uniqueness by full name
              .collect(Collectors.toMap(Schema::getFullName, Function.identity(), (x, y) -> x));

      List<Schema> schemas = new ArrayList<>(schemaMap.values());

      if (random.nextBoolean()) {
        org.apache.avro.Schema nullSchema = org.apache.avro.Schema.create(Schema.Type.NULL);
        schemas.add(nullSchema);
        Collections.shuffle(schemas, random.toJDKRandom());
      }

      return Schema.createUnion(schemas);
    }
  }

  static class EnumSchemaGenerator extends BaseSchemaGenerator {

    public static final EnumSchemaGenerator INSTANCE = new EnumSchemaGenerator();

    private static final Schema FRUITS =
        Schema.createEnum("Fruit", "", "example", Arrays.asList("banana", "apple", "pear"));

    private static final Schema STATUS =
        Schema.createEnum("Status", "", "example", Arrays.asList("OK", "ERROR", "WARNING"));

    @Override
    public Schema generate(final SourceOfRandomness random, final GenerationStatus status) {
      return random.choose(Arrays.asList(FRUITS, STATUS));
    }
  }

  abstract static class BaseSchemaGenerator extends Generator<Schema> {

    private static final GenerationStatus.Key<Integer> NESTING_KEY =
        new GenerationStatus.Key<>("nesting", Integer.class);

    private static final GenerationStatus.Key<String[]> BRANCH_KEY =
        new GenerationStatus.Key<>("branch", String[].class);

    BaseSchemaGenerator() {
      super(org.apache.avro.Schema.class);
    }

    void branchPush(GenerationStatus status, String value) {
      String[] current = status.valueOf(BRANCH_KEY).orElse(new String[0]);
      String[] next = ObjectArrays.concat(current, value);

      status.setValue(BRANCH_KEY, next);
    }

    void branchPop(GenerationStatus status) {
      String[] current = status.valueOf(BRANCH_KEY).orElse(new String[0]);
      String[] next = Arrays.copyOf(current, current.length - 1);

      status.setValue(BRANCH_KEY, next);
    }

    String branch(GenerationStatus status) {
      return Joiner.on("_").join(status.valueOf(BRANCH_KEY).orElse(new String[0]));
    }

    int nesting(GenerationStatus status) {
      return status.valueOf(NESTING_KEY).orElse(0);
    }

    void nestingInc(GenerationStatus status) {
      status.setValue(NESTING_KEY, nesting(status) + 1);
    }
  }
}
