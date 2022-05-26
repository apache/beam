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
package org.apache.beam.sdk.schemas.logicaltypes;

import java.util.Optional;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.LogicalType;
import org.apache.beam.sdk.util.PythonCallableSource;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A logical type for PythonCallableSource objects. */
@Experimental(Experimental.Kind.SCHEMAS)
public class PythonCallable implements LogicalType<PythonCallableSource, Row> {
  public static final String IDENTIFIER =
      SchemaApi.LogicalTypes.Enum.PYTHON_CALLABLE
          .getValueDescriptor()
          .getOptions()
          .getExtension(RunnerApi.beamUrn);
  private static final Schema SCHEMA =
      Schema.of(
          Schema.Field.of("callable", Schema.FieldType.STRING),
          Schema.Field.of("context", Schema.FieldType.STRING));

  @Override
  public String getIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Schema.@Nullable FieldType getArgumentType() {
    return null;
  }

  @Override
  public Schema.FieldType getBaseType() {
    return Schema.FieldType.row(SCHEMA);
  }

  @Override
  public @NonNull Row toBaseType(@NonNull PythonCallableSource input) {
    return Row.withSchema(SCHEMA)
        .addValue(input.getPythonCallableCode())
        .addValue(input.getPythonContextCode())
        .build();
  }

  @Override
  public @NonNull PythonCallableSource toInputType(@NonNull Row base) {
    return PythonCallableSource.of(Optional.ofNullable(base.getString("callable")).orElse(""))
        .withContext(Optional.ofNullable(base.getString("context")).orElse(""));
  }
}
