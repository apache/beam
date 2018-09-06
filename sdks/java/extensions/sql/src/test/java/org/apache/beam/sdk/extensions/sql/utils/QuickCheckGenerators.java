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
package org.apache.beam.sdk.extensions.sql.utils;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.apache.beam.sdk.schemas.Schema.toSchema;

import com.pholser.junit.quickcheck.generator.GenerationStatus;
import com.pholser.junit.quickcheck.generator.Generator;
import com.pholser.junit.quickcheck.random.SourceOfRandomness;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;

/** Field type generators invoked by {@link JUnitQuickcheck}. */
public class QuickCheckGenerators {

  private static final FieldTypeGenerator PRIMITIVE_TYPES = new PrimitiveTypes();
  private static final FieldTypeGenerator ARRAYS_OF_ANY = new Arrays();
  private static final FieldTypeGenerator MAPS_OF_ANY = new Maps();
  private static final FieldTypeGenerator ROWS = new Rows();
  private static final FieldTypeGenerator ANY_TYPE = new AnyFieldType();

  /** Generates a primitive {@link TypeName SQL type} randomly. */
  public static class PrimitiveTypes extends FieldTypeGenerator {
    private static final List<FieldType> PRIMITIVE_TYPES =
        java.util.Arrays.stream(TypeName.values())
            .filter(TypeName::isPrimitiveType)
            .map(FieldType::of)
            .collect(toList());

    @Override
    public FieldType generateFieldType(SourceOfRandomness random, GenerationStatus status) {
      return random.choose(PRIMITIVE_TYPES);
    }
  }

  /** Generates {@link TypeName#ARRAY SQL arrays} with random element type. */
  public static class Arrays extends FieldTypeGenerator {
    @Override
    public FieldType generateFieldType(SourceOfRandomness random, GenerationStatus status) {
      return FieldType.array(ANY_TYPE.generate(random, status));
    }
  }

  /**
   * Generates {@link TypeName#MAP SQL maps} with random primitive key type and random value type.
   */
  public static class Maps extends FieldTypeGenerator {
    @Override
    public FieldType generateFieldType(SourceOfRandomness random, GenerationStatus status) {
      return FieldType.map(
          PRIMITIVE_TYPES.generate(random, status), ANY_TYPE.generate(random, status));
    }
  }

  /** Generates {@link TypeName#ROW SQL rows} with random field types. */
  public static class Rows extends FieldTypeGenerator {
    @Override
    public FieldType generateFieldType(SourceOfRandomness random, GenerationStatus status) {
      // stop at 10 levels of nesting to avoid stack overflows
      FieldTypeGenerator rowFieldTypesGenerator =
          (nestingLevel(status) >= 10) ? PRIMITIVE_TYPES : ANY_TYPE;

      return FieldType.row(generateSchema(rowFieldTypesGenerator, random, status));
    }

    private Schema generateSchema(
        FieldTypeGenerator fieldTypeGenerator, SourceOfRandomness random, GenerationStatus status) {

      return IntStream.range(0, status.size() + 1)
          .mapToObj(
              i ->
                  Field.of("field_" + i, fieldTypeGenerator.generate(random, status))
                      .withNullable(true))
          .collect(toSchema());
    }
  }

  /** Generates a {@link FieldType}, randomly delegating to specific type generators. */
  public static class AnyFieldType extends FieldTypeGenerator {
    @Override
    public FieldType generateFieldType(SourceOfRandomness random, GenerationStatus status) {
      return random
          .choose(asList(PRIMITIVE_TYPES, ARRAYS_OF_ANY, MAPS_OF_ANY, ROWS))
          .generate(random, status);
    }
  }

  abstract static class FieldTypeGenerator extends Generator<FieldType> {
    static final GenerationStatus.Key<Integer> NESTING_KEY =
        new GenerationStatus.Key<>("nesting", Integer.class);

    FieldTypeGenerator() {
      super(FieldType.class);
    }

    @Override
    public FieldType generate(SourceOfRandomness random, GenerationStatus status) {
      incrementNesting(status);
      return generateFieldType(random, status);
    }

    protected abstract FieldType generateFieldType(
        SourceOfRandomness random, GenerationStatus status);

    int nestingLevel(GenerationStatus status) {
      return status.valueOf(NESTING_KEY).orElse(-1);
    }

    void incrementNesting(GenerationStatus status) {
      status.setValue(NESTING_KEY, nestingLevel(status) + 1);
    }
  }
}
