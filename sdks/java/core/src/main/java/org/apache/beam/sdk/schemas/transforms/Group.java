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
package org.apache.beam.sdk.schemas.transforms;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.utils.SelectHelpers;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * A generic grouping transform for schema {@link PCollection}s.
 *
 * <p>When used without a combiner, this transforms simply acts as a {@link GroupByKey} but without
 * the need for the user to explicitly extract the keys. For example, consider the following input
 * type:
 *
 * <pre>{@code @DefaultSchema(JavaFieldSchema.class)
 * public class UserPurchase {
 *   public String userId;
 *   public String country;
 *   public long cost;
 *   public double transactionDuration;
 * }
 *
 * PCollection<UserPurchase> purchases = readUserPurchases();
 * }</pre>
 *
 * <p>You can group all purchases by user and country as follows:
 *
 * <pre>{@code @DefaultSchema(JavaFieldSchema.class)
 * PCollection<KV<Row, Iterable<UserPurchase>> byUser =
 *   purchases.apply(Group.byFieldNames("userId', "country"));
 * }</pre>
 *
 * <p>However often an aggregation of some form is desired. The builder methods inside the Group
 * class allows building up separate aggregations for every field (or set of fields) on the input
 * schema, and generating an output schema based on these aggregations. For example:
 *
 * <pre>{@code
 * PCollection<KV<Row, Row>> aggregated = purchases
 *      .apply(Group.byFieldNames("userId', "country")
 *          .aggregateField("cost", Sum.ofLongs(), "total_cost")
 *          .aggregateField("cost", Top.<Long>largestLongsFn(10), "top_purchases")
 *          .aggregateField("cost", ApproximateQuantilesCombineFn.create(21),
 *              Field.of("transactionDurations", FieldType.array(FieldType.INT64)));
 * }</pre>
 *
 * <p>The result will be a new row schema containing the fields total_cost, top_purchases, and
 * transactionDurations, containing the sum of all purchases costs (for that user and country), the
 * top ten purchases, and a histogram of transaction durations.
 *
 * <p>Note that usually the field type can be automatically inferred from the {@link CombineFn}
 * passed in. However sometimes it cannot be inferred, due to Java type erasure, in which case a
 * {@link Field} object containing the field type must be passed in. This is currently the case for
 * ApproximateQuantilesCombineFn in the above example.
 */
@Experimental(Kind.SCHEMAS)
public class Group {
  /**
   * Returns a transform that groups all elements in the input {@link PCollection}. The returned
   * transform contains further builder methods to control how the grouping is done.
   */
  public static <T> Global<T> globally() {
    return new Global<>();
  }

  /**
   * Returns a transform that groups all elements in the input {@link PCollection} keyed by the list
   * of fields specified. The output of this transform will be a {@link KV} keyed by a {@link Row}
   * containing the specified extracted fields. The returned transform contains further builder
   * methods to control how the grouping is done.
   */
  public static <T> ByFields<T> byFieldNames(String... fieldNames) {
    return new ByFields<>(FieldAccessDescriptor.withFieldNames(fieldNames));
  }

  /** Same as {@link #byFieldNames(String...)}. */
  public static <T> ByFields<T> byFieldNames(Iterable<String> fieldNames) {
    return new ByFields<>(FieldAccessDescriptor.withFieldNames(fieldNames));
  }

  /**
   * Returns a transform that groups all elements in the input {@link PCollection} keyed by the list
   * of fields specified. The output of this transform will be a {@link KV} keyed by a {@link Row}
   * containing the specified extracted fields. The returned transform contains further builder
   * methods to control how the grouping is done.
   */
  public static <T> ByFields<T> byFieldIds(Integer... fieldIds) {
    return new ByFields<>(FieldAccessDescriptor.withFieldIds(fieldIds));
  }

  /** Same as {@link #byFieldIds(Integer...)}. */
  public static <T> ByFields<T> byFieldIds(Iterable<Integer> fieldIds) {
    return new ByFields<>(FieldAccessDescriptor.withFieldIds(fieldIds));
  }

  /**
   * Returns a transform that groups all elements in the input {@link PCollection} keyed by the
   * fields specified. The output of this transform will be a {@link KV} keyed by a {@link Row}
   * containing the specified extracted fields. The returned transform contains further builder
   * methods to control how the grouping is done.
   */
  public static <T> ByFields<T> byFieldAccessDescriptor(FieldAccessDescriptor fieldAccess) {
    return new ByFields<>(fieldAccess);
  }

  /** A {@link PTransform} for doing global aggregations on schema PCollections. */
  public static class Global<InputT>
      extends PTransform<PCollection<InputT>, PCollection<Iterable<InputT>>> {
    /**
     * Aggregate the grouped data using the specified {@link CombineFn}. The resulting {@link
     * PCollection} will have type OutputT.
     */
    public <OutputT> CombineGlobally<InputT, OutputT> aggregate(
        CombineFn<InputT, ?, OutputT> combineFn) {
      return new CombineGlobally<>(combineFn);
    }

    /**
     * Build up an aggregation function over the input elements.
     *
     * <p>This method specifies an aggregation over single field of the input. The union of all
     * calls to aggregateField and aggregateFields will determine the output schema.
     */
    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsGlobally<InputT> aggregateField(
        String inputFieldName,
        CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
        String outputFieldName) {
      return new CombineFieldsGlobally<>(
          SchemaAggregateFn.<InputT>create()
              .aggregateFields(
                  FieldAccessDescriptor.withFieldNames(inputFieldName), fn, outputFieldName));
    }

    /** The same as {@link #aggregateField} but using field id. */
    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsGlobally<InputT> aggregateField(
        int inputFieldId,
        CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
        String outputFieldName) {
      return new CombineFieldsGlobally<>(
          SchemaAggregateFn.<InputT>create()
              .aggregateFields(
                  FieldAccessDescriptor.withFieldIds(inputFieldId), fn, outputFieldName));
    }

    /**
     * Build up an aggregation function over the input elements.
     *
     * <p>This method specifies an aggregation over single field of the input. The union of all
     * calls to aggregateField and aggregateFields will determine the output schema.
     */
    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsGlobally<InputT> aggregateField(
        String inputFieldName,
        CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
        Field outputField) {
      return new CombineFieldsGlobally<>(
          SchemaAggregateFn.<InputT>create()
              .aggregateFields(
                  FieldAccessDescriptor.withFieldNames(inputFieldName), fn, outputField));
    }

    /** The same as {@link #aggregateField} but using field id. */
    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsGlobally<InputT> aggregateField(
        int inputFielId, CombineFn<CombineInputT, AccumT, CombineOutputT> fn, Field outputField) {
      return new CombineFieldsGlobally<>(
          SchemaAggregateFn.<InputT>create()
              .aggregateFields(FieldAccessDescriptor.withFieldIds(inputFielId), fn, outputField));
    }

    /**
     * Build up an aggregation function over the input elements.
     *
     * <p>This method specifies an aggregation over multiple fields of the input. The union of all
     * calls to aggregateField and aggregateFields will determine the output schema.
     *
     * <p>Field types in the output schema will be inferred from the provided combine function.
     * Sometimes the field type cannot be inferred due to Java's type erasure. In that case, use the
     * overload that allows setting the output field type explicitly.
     */
    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsGlobally<InputT> aggregateFields(
        List<String> inputFieldNames,
        CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
        String outputFieldName) {
      return aggregateFields(
          FieldAccessDescriptor.withFieldNames(inputFieldNames), fn, outputFieldName);
    }

    /** The same as {@link #aggregateFields} but with field ids. */
    public <CombineInputT, AccumT, CombineOutputT>
        CombineFieldsGlobally<InputT> aggregateFieldsById(
            List<Integer> inputFieldIds,
            CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
            String outputFieldName) {
      return aggregateFields(
          FieldAccessDescriptor.withFieldIds(inputFieldIds), fn, outputFieldName);
    }

    /**
     * Build up an aggregation function over the input elements.
     *
     * <p>This method specifies an aggregation over multiple fields of the input. The union of all
     * calls to aggregateField and aggregateFields will determine the output schema.
     *
     * <p>Field types in the output schema will be inferred from the provided combine function.
     * Sometimes the field type cannot be inferred due to Java's type erasure. In that case, use the
     * overload that allows setting the output field type explicitly.
     */
    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsGlobally<InputT> aggregateFields(
        FieldAccessDescriptor fieldsToAggregate,
        CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
        String outputFieldName) {
      return new CombineFieldsGlobally<>(
          SchemaAggregateFn.<InputT>create()
              .aggregateFields(fieldsToAggregate, fn, outputFieldName));
    }

    /**
     * Build up an aggregation function over the input elements.
     *
     * <p>This method specifies an aggregation over multiple fields of the input. The union of all
     * calls to aggregateField and aggregateFields will determine the output schema.
     */
    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsGlobally<InputT> aggregateFields(
        List<String> inputFieldNames,
        CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
        Field outputField) {
      return aggregateFields(
          FieldAccessDescriptor.withFieldNames(inputFieldNames), fn, outputField);
    }

    public <CombineInputT, AccumT, CombineOutputT>
        CombineFieldsGlobally<InputT> aggregateFieldsById(
            List<Integer> inputFieldIds,
            CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
            Field outputField) {
      return aggregateFields(FieldAccessDescriptor.withFieldIds(inputFieldIds), fn, outputField);
    }

    /**
     * Build up an aggregation function over the input elements.
     *
     * <p>This method specifies an aggregation over multiple fields of the input. The union of all
     * calls to aggregateField and aggregateFields will determine the output schema.
     */
    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsGlobally<InputT> aggregateFields(
        FieldAccessDescriptor fieldsToAggregate,
        CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
        Field outputField) {
      return new CombineFieldsGlobally<>(
          SchemaAggregateFn.<InputT>create().aggregateFields(fieldsToAggregate, fn, outputField));
    }

    @Override
    public PCollection<Iterable<InputT>> expand(PCollection<InputT> input) {
      return input
          .apply(WithKeys.of((Void) null))
          .apply(GroupByKey.create())
          .apply(Values.create());
    }
  }

  /** a {@link PTransform} that does a global combine using a provider {@link CombineFn}. */
  public static class CombineGlobally<InputT, OutputT>
      extends PTransform<PCollection<InputT>, PCollection<OutputT>> {
    final CombineFn<InputT, ?, OutputT> combineFn;

    CombineGlobally(CombineFn<InputT, ?, OutputT> combineFn) {
      this.combineFn = combineFn;
    }

    @Override
    public PCollection<OutputT> expand(PCollection<InputT> input) {
      return input.apply(Combine.globally(combineFn));
    }
  }

  /**
   * a {@link PTransform} that does a global combine using an aggregation built up by calls to
   * aggregateField and aggregateFields. The output of this transform will have a schema that is
   * determined by the output types of all the composed combiners.
   */
  public static class CombineFieldsGlobally<InputT>
      extends PTransform<PCollection<InputT>, PCollection<Row>> {
    private final SchemaAggregateFn.Inner<InputT> schemaAggregateFn;

    CombineFieldsGlobally(SchemaAggregateFn.Inner<InputT> schemaAggregateFn) {
      this.schemaAggregateFn = schemaAggregateFn;
    }

    /**
     * Build up an aggregation function over the input elements.
     *
     * <p>This method specifies an aggregation over single field of the input. The union of all
     * calls to aggregateField and aggregateFields will determine the output schema.
     *
     * <p>Field types in the output schema will be inferred from the provided combine function.
     * Sometimes the field type cannot be inferred due to Java's type erasure. In that case, use the
     * overload that allows setting the output field type explicitly.
     */
    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsGlobally<InputT> aggregateField(
        String inputFieldName,
        CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
        String outputFieldName) {
      return new CombineFieldsGlobally<>(
          schemaAggregateFn.aggregateFields(
              FieldAccessDescriptor.withFieldNames(inputFieldName), fn, outputFieldName));
    }

    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsGlobally<InputT> aggregateField(
        int inputFieldId,
        CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
        String outputFieldName) {
      return new CombineFieldsGlobally<>(
          schemaAggregateFn.aggregateFields(
              FieldAccessDescriptor.withFieldIds(inputFieldId), fn, outputFieldName));
    }

    /**
     * Build up an aggregation function over the input elements.
     *
     * <p>This method specifies an aggregation over single field of the input. The union of all
     * calls to aggregateField and aggregateFields will determine the output schema.
     */
    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsGlobally<InputT> aggregateField(
        String inputFieldName,
        CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
        Field outputField) {
      return new CombineFieldsGlobally<>(
          schemaAggregateFn.aggregateFields(
              FieldAccessDescriptor.withFieldNames(inputFieldName), fn, outputField));
    }

    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsGlobally<InputT> aggregateField(
        int inputFieldId, CombineFn<CombineInputT, AccumT, CombineOutputT> fn, Field outputField) {
      return new CombineFieldsGlobally<>(
          schemaAggregateFn.aggregateFields(
              FieldAccessDescriptor.withFieldIds(inputFieldId), fn, outputField));
    }

    /**
     * Build up an aggregation function over the input elements.
     *
     * <p>This method specifies an aggregation over multiple fields of the input. The union of all
     * calls to aggregateField and aggregateFields will determine the output schema.
     *
     * <p>Field types in the output schema will be inferred from the provided combine function.
     * Sometimes the field type cannot be inferred due to Java's type erasure. In that case, use the
     * overload that allows setting the output field type explicitly.
     */
    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsGlobally<InputT> aggregateFields(
        List<String> inputFieldNames,
        CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
        String outputFieldName) {
      return aggregateFields(
          FieldAccessDescriptor.withFieldNames(inputFieldNames), fn, outputFieldName);
    }

    public <CombineInputT, AccumT, CombineOutputT>
        CombineFieldsGlobally<InputT> aggregateFieldsById(
            List<Integer> inputFieldIds,
            CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
            String outputFieldName) {
      return aggregateFields(
          FieldAccessDescriptor.withFieldIds(inputFieldIds), fn, outputFieldName);
    }

    /**
     * Build up an aggregation function over the input elements.
     *
     * <p>This method specifies an aggregation over multiple fields of the input. The union of all
     * calls to aggregateField and aggregateFields will determine the output schema.
     *
     * <p>Field types in the output schema will be inferred from the provided combine function.
     * Sometimes the field type cannot be inferred due to Java's type erasure. In that case, use the
     * overload that allows setting the output field type explicitly.
     */
    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsGlobally<InputT> aggregateFields(
        FieldAccessDescriptor fieldAccessDescriptor,
        CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
        String outputFieldName) {
      return new CombineFieldsGlobally<>(
          schemaAggregateFn.aggregateFields(fieldAccessDescriptor, fn, outputFieldName));
    }

    /**
     * Build up an aggregation function over the input elements.
     *
     * <p>This method specifies an aggregation over multiple fields of the input. The union of all
     * calls to aggregateField and aggregateFields will determine the output schema.
     */
    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsGlobally<InputT> aggregateFields(
        List<String> inputFieldNames,
        CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
        Field outputField) {
      return aggregateFields(
          FieldAccessDescriptor.withFieldNames(inputFieldNames), fn, outputField);
    }

    public <CombineInputT, AccumT, CombineOutputT>
        CombineFieldsGlobally<InputT> aggregateFieldsById(
            List<Integer> inputFieldIds,
            CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
            Field outputField) {
      return aggregateFields(FieldAccessDescriptor.withFieldIds(inputFieldIds), fn, outputField);
    }

    /**
     * Build up an aggregation function over the input elements.
     *
     * <p>This method specifies an aggregation over multiple fields of the input. The union of all
     * calls to aggregateField and aggregateFields will determine the output schema.
     */
    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsGlobally<InputT> aggregateFields(
        FieldAccessDescriptor fieldAccessDescriptor,
        CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
        Field outputField) {
      return new CombineFieldsGlobally<>(
          schemaAggregateFn.aggregateFields(fieldAccessDescriptor, fn, outputField));
    }

    @Override
    public PCollection<Row> expand(PCollection<InputT> input) {
      SchemaAggregateFn.Inner<InputT> fn =
          schemaAggregateFn.withSchema(input.getSchema(), input.getToRowFunction());
      return input.apply(Combine.globally(fn)).setRowSchema(fn.getOutputSchema());
    }
  }

  /**
   * a {@link PTransform} that groups schema elements based on the given fields.
   *
   * <p>The output of this transform is a {@link KV} where the key type is a {@link Row} containing
   * the extracted fields.
   */
  public static class ByFields<InputT>
      extends PTransform<PCollection<InputT>, PCollection<KV<Row, Iterable<InputT>>>> {
    private final FieldAccessDescriptor fieldAccessDescriptor;
    @Nullable private Schema keySchema = null;

    private ByFields(FieldAccessDescriptor fieldAccessDescriptor) {
      this.fieldAccessDescriptor = fieldAccessDescriptor;
    }

    Schema getKeySchema() {
      return keySchema;
    }

    /**
     * Aggregate the grouped data using the specified {@link CombineFn}. The resulting {@link
     * PCollection} will have type {@code PCollection<KV<Row, OutputT>>}.
     */
    public <OutputT> CombineByFields<InputT, OutputT> aggregate(
        CombineFn<InputT, ?, OutputT> combineFn) {
      return new CombineByFields<>(this, combineFn);
    }

    /**
     * Build up an aggregation function over the input elements.
     *
     * <p>This method specifies an aggregation over single field of the input. The union of all
     * calls to aggregateField and aggregateFields will determine the output schema.
     *
     * <p>Field types in the output schema will be inferred from the provided combine function.
     * Sometimes the field type cannot be inferred due to Java's type erasure. In that case, use the
     * overload that allows setting the output field type explicitly.
     */
    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsByFields<InputT> aggregateField(
        String inputFieldName,
        CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
        String outputFieldName) {
      return new CombineFieldsByFields<>(
          this,
          SchemaAggregateFn.<InputT>create()
              .aggregateFields(
                  FieldAccessDescriptor.withFieldNames(inputFieldName), fn, outputFieldName));
    }

    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsByFields<InputT> aggregateField(
        int inputFieldId,
        CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
        String outputFieldName) {
      return new CombineFieldsByFields<>(
          this,
          SchemaAggregateFn.<InputT>create()
              .aggregateFields(
                  FieldAccessDescriptor.withFieldIds(inputFieldId), fn, outputFieldName));
    }

    /**
     * Build up an aggregation function over the input elements.
     *
     * <p>This method specifies an aggregation over single field of the input. The union of all
     * calls to aggregateField and aggregateFields will determine the output schema.
     */
    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsByFields<InputT> aggregateField(
        String inputFieldName,
        CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
        Field outputField) {
      return new CombineFieldsByFields<>(
          this,
          SchemaAggregateFn.<InputT>create()
              .aggregateFields(
                  FieldAccessDescriptor.withFieldNames(inputFieldName), fn, outputField));
    }

    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsByFields<InputT> aggregateField(
        int inputFieldId, CombineFn<CombineInputT, AccumT, CombineOutputT> fn, Field outputField) {
      return new CombineFieldsByFields<>(
          this,
          SchemaAggregateFn.<InputT>create()
              .aggregateFields(FieldAccessDescriptor.withFieldIds(inputFieldId), fn, outputField));
    }

    /**
     * Build up an aggregation function over the input elements.
     *
     * <p>This method specifies an aggregation over multiple fields of the input. The union of all
     * calls to aggregateField and aggregateFields will determine the output schema.
     *
     * <p>Field types in the output schema will be inferred from the provided combine function.
     * Sometimes the field type cannot be inferred due to Java's type erasure. In that case, use the
     * overload that allows setting the output field type explicitly.
     */
    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsByFields<InputT> aggregateFields(
        List<String> inputFieldNames,
        CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
        String outputFieldName) {
      return aggregateFields(
          FieldAccessDescriptor.withFieldNames(inputFieldNames), fn, outputFieldName);
    }

    public <CombineInputT, AccumT, CombineOutputT>
        CombineFieldsByFields<InputT> aggregateFieldsById(
            List<Integer> inputFieldIds,
            CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
            String outputFieldName) {
      return aggregateFields(
          FieldAccessDescriptor.withFieldIds(inputFieldIds), fn, outputFieldName);
    }

    /**
     * Build up an aggregation function over the input elements.
     *
     * <p>This method specifies an aggregation over multiple fields of the input. The union of all
     * calls to aggregateField and aggregateFields will determine the output schema.
     *
     * <p>Field types in the output schema will be inferred from the provided combine function.
     * Sometimes the field type cannot be inferred due to Java's type erasure. In that case, use the
     * overload that allows setting the output field type explicitly.
     */
    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsByFields<InputT> aggregateFields(
        FieldAccessDescriptor fieldsToAggregate,
        CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
        String outputFieldName) {
      return new CombineFieldsByFields<>(
          this,
          SchemaAggregateFn.<InputT>create()
              .aggregateFields(fieldsToAggregate, fn, outputFieldName));
    }

    /**
     * Build up an aggregation function over the input elements.
     *
     * <p>This method specifies an aggregation over multiple fields of the input. The union of all
     * calls to aggregateField and aggregateFields will determine the output schema.
     */
    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsByFields<InputT> aggregateFields(
        List<String> inputFieldNames,
        CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
        Field outputField) {
      return aggregateFields(
          FieldAccessDescriptor.withFieldNames(inputFieldNames), fn, outputField);
    }

    public <CombineInputT, AccumT, CombineOutputT>
        CombineFieldsByFields<InputT> aggregateFieldsById(
            List<Integer> inputFieldIds,
            CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
            Field outputField) {
      return aggregateFields(FieldAccessDescriptor.withFieldIds(inputFieldIds), fn, outputField);
    }

    /**
     * Build up an aggregation function over the input elements.
     *
     * <p>This method specifies an aggregation over multiple fields of the input. The union of all
     * calls to aggregateField and aggregateFields will determine the output schema.
     */
    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsByFields<InputT> aggregateFields(
        FieldAccessDescriptor fieldsToAggregate,
        CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
        Field outputField) {
      return new CombineFieldsByFields<>(
          this,
          SchemaAggregateFn.<InputT>create().aggregateFields(fieldsToAggregate, fn, outputField));
    }

    @Override
    public PCollection<KV<Row, Iterable<InputT>>> expand(PCollection<InputT> input) {
      Schema schema = input.getSchema();
      FieldAccessDescriptor resolved = fieldAccessDescriptor.resolve(schema);
      keySchema = SelectHelpers.getOutputSchema(schema, resolved);
      return input
          .apply(
              "Group by fields",
              ParDo.of(
                  new DoFn<InputT, KV<Row, InputT>>() {
                    @ProcessElement
                    public void process(
                        @Element InputT element,
                        @Element Row row,
                        OutputReceiver<KV<Row, InputT>> o) {
                      o.output(
                          KV.of(
                              SelectHelpers.selectRow(row, resolved, schema, keySchema), element));
                    }
                  }))
          .setCoder(KvCoder.of(SchemaCoder.of(keySchema), input.getCoder()))
          .apply(GroupByKey.create());
    }
  }

  /**
   * a {@link PTransform} that does a per0-key combine using a specified {@link CombineFn}.
   *
   * <p>The output of this transform is a {@code <KV<Row, OutputT>>} where the key type is a {@link
   * Row} containing the extracted fields.
   */
  public static class CombineByFields<InputT, OutputT>
      extends PTransform<PCollection<InputT>, PCollection<KV<Row, OutputT>>> {
    private final ByFields<InputT> byFields;
    private final CombineFn<InputT, ?, OutputT> combineFn;

    CombineByFields(ByFields<InputT> byFields, CombineFn<InputT, ?, OutputT> combineFn) {
      this.byFields = byFields;
      this.combineFn = combineFn;
    }

    @Override
    public PCollection<KV<Row, OutputT>> expand(PCollection<InputT> input) {
      return input.apply(byFields).apply(Combine.groupedValues(combineFn));
    }
  }

  /**
   * a {@link PTransform} that does a per-key combine using an aggregation built up by calls to
   * aggregateField and aggregateFields. The output of this transform will have a schema that is
   * determined by the output types of all the composed combiners.
   */
  public static class CombineFieldsByFields<InputT>
      extends PTransform<PCollection<InputT>, PCollection<KV<Row, Row>>> {
    private final ByFields<InputT> byFields;
    private final SchemaAggregateFn.Inner<InputT> schemaAggregateFn;

    CombineFieldsByFields(
        ByFields<InputT> byFields, SchemaAggregateFn.Inner<InputT> schemaAggregateFn) {
      this.byFields = byFields;
      this.schemaAggregateFn = schemaAggregateFn;
    }

    /**
     * Build up an aggregation function over the input elements.
     *
     * <p>This method specifies an aggregation over single field of the input. The union of all
     * calls to aggregateField and aggregateFields will determine the output schema.
     *
     * <p>Field types in the output schema will be inferred from the provided combine function.
     * Sometimes the field type cannot be inferred due to Java's type erasure. In that case, use the
     * overload that allows setting the output field type explicitly.
     */
    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsByFields<InputT> aggregateField(
        String inputFieldName,
        CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
        String outputFieldName) {
      return new CombineFieldsByFields<>(
          byFields,
          schemaAggregateFn.aggregateFields(
              FieldAccessDescriptor.withFieldNames(inputFieldName), fn, outputFieldName));
    }

    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsByFields<InputT> aggregateField(
        int inputFieldId,
        CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
        String outputFieldName) {
      return new CombineFieldsByFields<>(
          byFields,
          schemaAggregateFn.aggregateFields(
              FieldAccessDescriptor.withFieldIds(inputFieldId), fn, outputFieldName));
    }

    /**
     * Build up an aggregation function over the input elements.
     *
     * <p>This method specifies an aggregation over single field of the input. The union of all
     * calls to aggregateField and aggregateFields will determine the output schema.
     */
    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsByFields<InputT> aggregateField(
        String inputFieldName,
        CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
        Field outputField) {
      return new CombineFieldsByFields<>(
          byFields,
          schemaAggregateFn.aggregateFields(
              FieldAccessDescriptor.withFieldNames(inputFieldName), fn, outputField));
    }

    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsByFields<InputT> aggregateField(
        int inputFieldId, CombineFn<CombineInputT, AccumT, CombineOutputT> fn, Field outputField) {
      return new CombineFieldsByFields<>(
          byFields,
          schemaAggregateFn.aggregateFields(
              FieldAccessDescriptor.withFieldIds(inputFieldId), fn, outputField));
    }

    /**
     * Build up an aggregation function over the input elements.
     *
     * <p>This method specifies an aggregation over multiple fields of the input. The union of all
     * calls to aggregateField and aggregateFields will determine the output schema.
     *
     * <p>Field types in the output schema will be inferred from the provided combine function.
     * Sometimes the field type cannot be inferred due to Java's type erasure. In that case, use the
     * overload that allows setting the output field type explicitly.
     */
    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsByFields<InputT> aggregateFields(
        List<String> inputFieldNames,
        CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
        String outputFieldName) {
      return aggregateFields(
          FieldAccessDescriptor.withFieldNames(inputFieldNames), fn, outputFieldName);
    }

    /**
     * Build up an aggregation function over the input elements.
     *
     * <p>This method specifies an aggregation over multiple fields of the input. The union of all
     * calls to aggregateField and aggregateFields will determine the output schema.
     *
     * <p>Field types in the output schema will be inferred from the provided combine function.
     * Sometimes the field type cannot be inferred due to Java's type erasure. In that case, use the
     * overload that allows setting the output field type explicitly.
     */
    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsByFields<InputT> aggregateFields(
        FieldAccessDescriptor fieldsToAggregate,
        CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
        String outputFieldName) {
      return new CombineFieldsByFields<>(
          byFields, schemaAggregateFn.aggregateFields(fieldsToAggregate, fn, outputFieldName));
    }

    /**
     * Build up an aggregation function over the input elements.
     *
     * <p>This method specifies an aggregation over multiple fields of the input. The union of all
     * calls to aggregateField and aggregateFields will determine the output schema.
     */
    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsByFields<InputT> aggregateFields(
        List<String> inputFieldNames,
        CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
        Field outputField) {
      return aggregateFields(
          FieldAccessDescriptor.withFieldNames(inputFieldNames), fn, outputField);
    }

    public <CombineInputT, AccumT, CombineOutputT>
        CombineFieldsByFields<InputT> aggregateFieldsById(
            List<Integer> inputFieldIds,
            CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
            Field outputField) {
      return aggregateFields(FieldAccessDescriptor.withFieldIds(inputFieldIds), fn, outputField);
    }

    /**
     * Build up an aggregation function over the input elements.
     *
     * <p>This method specifies an aggregation over multiple fields of the input. The union of all
     * calls to aggregateField and aggregateFields will determine the output schema.
     */
    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsByFields<InputT> aggregateFields(
        FieldAccessDescriptor fieldsToAggregate,
        CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
        Field outputField) {
      return new CombineFieldsByFields<>(
          byFields, schemaAggregateFn.aggregateFields(fieldsToAggregate, fn, outputField));
    }

    @Override
    public PCollection<KV<Row, Row>> expand(PCollection<InputT> input) {
      SchemaAggregateFn.Inner<InputT> fn =
          schemaAggregateFn.withSchema(input.getSchema(), input.getToRowFunction());
      return input.apply(byFields).apply(Combine.groupedValues(fn));
    }
  }
}
