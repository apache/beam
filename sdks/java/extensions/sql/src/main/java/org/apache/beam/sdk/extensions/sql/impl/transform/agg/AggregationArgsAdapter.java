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
package org.apache.beam.sdk.extensions.sql.impl.transform.agg;

import static java.util.stream.Collectors.toList;
import static org.apache.beam.sdk.schemas.Schema.toSchema;

import java.io.Serializable;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.values.Row;

/**
 * Utility class to extract arguments from the input Row to match the expected input of the {@link
 * CombineFn}.
 */
class AggregationArgsAdapter {

  /**
   * Creates an args adapter based on the args list and input schema.
   *
   * @param argList indices of fields that are specified as arguments for an aggregation call.
   * @param inputSchema input row that will be passed into the aggregation
   */
  static ArgsAdapter of(List<Integer> argList, Schema inputSchema) {
    if (argList.size() == 0) {
      return ZeroArgsAdapter.INSTANCE;
    } else if (argList.size() == 1) {
      return new SingleArgAdapter(inputSchema, argList);
    } else {
      return new MultiArgsAdapter(inputSchema, argList);
    }
  }

  /**
   * If SQL aggregation call doesn't have actual arguments, we pass an empty row to it.
   *
   * <p>This is for cases like COUNT(1) which doesn't take any arguments, or COUNT(*) that is a
   * special case that returns the same result. In both of these cases we should count all Rows no
   * matter whether they have NULLs or not.
   *
   * <p>This is a special case of the MultiArgsAdapter below.
   */
  static class ZeroArgsAdapter implements ArgsAdapter {
    private static final Schema EMPTY_SCHEMA = Schema.builder().build();
    private static final Row EMPTY_ROW = Row.withSchema(EMPTY_SCHEMA).build();
    private static final Coder<Row> EMPTY_ROW_CODER = SchemaCoder.of(EMPTY_SCHEMA);

    static final ZeroArgsAdapter INSTANCE = new ZeroArgsAdapter();

    /** Extracts the value from the first field of a row. */
    @Nullable
    @Override
    public Object getArgsValues(Row input) {
      return EMPTY_ROW;
    }

    /** Coder for the first field of a row. */
    @Override
    public Coder getArgsValuesCoder() {
      return EMPTY_ROW_CODER;
    }
  }

  /**
   * If SQL aggregation call has a single argument (e.g. MAX), we extract its raw value to pass to
   * the delegate {@link CombineFn}.
   */
  static class SingleArgAdapter implements ArgsAdapter {
    Schema sourceSchema;
    List<Integer> argsIndicies;

    public SingleArgAdapter(Schema sourceSchema, List<Integer> argsIndicies) {
      this.sourceSchema = sourceSchema;
      this.argsIndicies = argsIndicies;
    }

    /**
     * Args indices contain a single element with the index of a field that SQL call specifies. Here
     * we extract the value of that field from the input row.
     */
    @Nullable
    @Override
    public Object getArgsValues(Row input) {
      return input.getValue(argsIndicies.get(0));
    }

    /** Coder for the field of a row used as an argument. */
    @Override
    public Coder getArgsValuesCoder() {
      int fieldIndex = argsIndicies.get(0);
      return RowCoder.coderForFieldType(sourceSchema.getField(fieldIndex).getType());
    }
  }

  /**
   * If SQL aggregation call has multiple arguments (e.g. COVAR_POP), we extract the fields
   * specified in the arguments, then combine them into a row, and then pass into the delegate
   * {@link CombineFn}.
   */
  static class MultiArgsAdapter implements ArgsAdapter {
    Schema sourceSchema;
    List<Integer> argsIndicies;

    MultiArgsAdapter(Schema sourceSchema, List<Integer> argsIndicies) {
      this.sourceSchema = sourceSchema;
      this.argsIndicies = argsIndicies;
    }

    /**
     * Extract the sub-row with the fields specified in the arguments. If args values contain nulls,
     * return null.
     */
    @Nullable
    @Override
    public Object getArgsValues(Row input) {
      List<Object> argsValues = argsIndicies.stream().map(input::getValue).collect(toList());

      if (argsValues.contains(null)) {
        return null;
      }

      Schema argsSchema =
          argsIndicies
              .stream()
              .map(fieldIndex -> input.getSchema().getField(fieldIndex))
              .collect(toSchema());

      return Row.withSchema(argsSchema).addValues(argsValues).build();
    }

    /** Schema coder of the sub-row specified by the fields in the arguments list. */
    @Override
    public Coder getArgsValuesCoder() {
      Schema argsSchema =
          argsIndicies
              .stream()
              .map(fieldIndex -> sourceSchema.getField(fieldIndex))
              .collect(toSchema());

      return SchemaCoder.of(argsSchema);
    }
  }

  interface ArgsAdapter extends Serializable {
    @Nullable
    Object getArgsValues(Row input);

    Coder getArgsValuesCoder();
  }
}
