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

package org.apache.beam.sdk.extensions.sql;


import static org.apache.beam.sdk.extensions.sql.QueryValidationHelper.validateQuery;
import static org.apache.beam.sdk.extensions.sql.SchemaHelper.toRows;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;

/**
 * A {@link PTransform} representing an execution plan for a SQL query.
 *
 * <p>The table names in the input {@code PCollectionTuple} are only valid during the current
 * query.
 */
@AutoValue
public abstract class QueryTransform extends PTransform<PInput, PCollection<Row>> {
  static final String PCOLLECTION_NAME = "PCOLLECTION";

  abstract String queryString();
  abstract List<UdfDefinition> udfDefinitions();
  abstract List<UdafDefinition> udafDefinitions();

  @Override
  public PCollection<Row> expand(PInput input) {
    PCollectionTuple inputTuple = toPCollectionTuple(input);

    BeamSqlEnv sqlEnv = new BeamSqlEnv();

    if (input instanceof PCollection) {
      validateQuery(sqlEnv, queryString());
    }

    sqlEnv.registerPCollectionTuple(inputTuple);
    registerFunctions(sqlEnv);

    try {
      return
          inputTuple.apply(
          sqlEnv
              .getPlanner()
              .convertToBeamRel(queryString())
              .toPTransform());
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private PCollectionTuple toPCollectionTuple(PInput inputs) {
    return (inputs instanceof PCollection)
        ? PCollectionTuple.of(new TupleTag<>(PCOLLECTION_NAME), toRows(inputs))
        : tupleOfAllInputs(inputs.getPipeline(), inputs.expand());
  }

  private PCollectionTuple tupleOfAllInputs(
      Pipeline pipeline,
      Map<TupleTag<?>, PValue> taggedInputs) {

    PCollectionTuple tuple = PCollectionTuple.empty(pipeline);

    for (Map.Entry<TupleTag<?>, PValue> input : taggedInputs.entrySet()) {
      tuple = tuple.and(
          new TupleTag<>(input.getKey().getId()),
          toRows(input.getValue()));
    }

    return tuple;
  }

  private void registerFunctions(BeamSqlEnv sqlEnv) {
    udfDefinitions()
        .forEach(udf -> sqlEnv.registerUdf(udf.udfName(), udf.clazz(), udf.methodName()));

    udafDefinitions()
        .forEach(udaf -> sqlEnv.registerUdaf(udaf.udafName(), udaf.combineFn()));
  }

  /**
   * Creates a {@link QueryTransform} with SQL {@code queryString}.
   */
  public static QueryTransform withQueryString(String queryString) {
    return
        builder()
            .setQueryString(queryString)
            .setUdafDefinitions(Collections.emptyList())
            .setUdfDefinitions(Collections.emptyList())
            .build();
  }

  /**
   * register a UDF function used in this query.
   *
   * <p>Refer to {@link BeamSqlUdf} for more about how to implement a UDF in BeamSql.
   */
  public QueryTransform registerUdf(String functionName, Class<? extends BeamSqlUdf> clazz) {
    return registerUdf(functionName, clazz, BeamSqlUdf.UDF_METHOD);
  }

  /**
   * Register {@link SerializableFunction} as a UDF function used in this query.
   * Note, {@link SerializableFunction} must have a constructor without arguments.
   */
  public QueryTransform registerUdf(String functionName, SerializableFunction sfn) {
    return registerUdf(functionName, sfn.getClass(), "apply");
  }

  private QueryTransform registerUdf(String functionName, Class<?> clazz, String method) {
    ImmutableList<UdfDefinition> newUdfDefinitions =
        ImmutableList
            .<UdfDefinition>builder()
            .addAll(udfDefinitions())
            .add(UdfDefinition.of(functionName, clazz, method))
            .build();

    return toBuilder().setUdfDefinitions(newUdfDefinitions).build();
  }

  /**
   * register a {@link Combine.CombineFn} as UDAF function used in this query.
   */
  public QueryTransform registerUdaf(String functionName, Combine.CombineFn combineFn) {
    ImmutableList<UdafDefinition> newUdafs =
        ImmutableList
            .<UdafDefinition>builder()
            .addAll(udafDefinitions())
            .add(UdafDefinition.of(functionName, combineFn))
            .build();

    return toBuilder().setUdafDefinitions(newUdafs).build();
  }

  abstract Builder toBuilder();

  static Builder builder() {
    return new AutoValue_QueryTransform.Builder();
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setQueryString(String queryString);
    abstract Builder setUdfDefinitions(List<UdfDefinition> udfDefinitions);
    abstract Builder setUdafDefinitions(List<UdafDefinition> udafDefinitions);

    abstract QueryTransform build();
  }

  @AutoValue
  abstract static class UdfDefinition {
    abstract String udfName();
    abstract Class<?> clazz();
    abstract String methodName();

    static UdfDefinition of(String udfName, Class<?> clazz, String methodName) {
      return new AutoValue_QueryTransform_UdfDefinition(udfName, clazz, methodName);
    }
  }

  @AutoValue
  abstract static class UdafDefinition {
    abstract String udafName();
    abstract Combine.CombineFn combineFn();

    static UdafDefinition of(String udafName, Combine.CombineFn combineFn) {
      return new AutoValue_QueryTransform_UdafDefinition(udafName, combineFn);
    }
  }
}
