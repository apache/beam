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

import com.google.auto.value.AutoValue;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.utils.RowSelector;
import org.apache.beam.sdk.schemas.utils.SelectHelpers;
import org.apache.beam.sdk.schemas.utils.SelectHelpers.RowSelectorContainer;
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
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;

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
 * PCollection<Row> byUser = purchases.apply(Group.byFieldNames("userId', "country"));
 * }</pre>
 *
 * <p>However often an aggregation of some form is desired. The builder methods inside the Group
 * class allows building up separate aggregations for every field (or set of fields) on the input
 * schema, and generating an output schema based on these aggregations. For example:
 *
 * <pre>{@code
 * PCollection<Row> aggregated = purchases
 *      .apply(Group.byFieldNames("userId', "country")
 *          .aggregateField("cost", Sum.ofLongs(), "total_cost")
 *          .aggregateField("cost", Top.<Long>largestLongsFn(10), "top_purchases")
 *          .aggregateField("cost", ApproximateQuantilesCombineFn.create(21),
 *              Field.of("transactionDurations", FieldType.array(FieldType.INT64)));
 * }</pre>
 *
 * <p>The result will be a new row schema containing the fields total_cost, top_purchases, and
 * transactionDurations, containing the sum of all purchases costs (for that user and country), the
 * top ten purchases, and a histogram of transaction durations. The schema will also contain a key
 * field, which will be a row containing userId and country.
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
    return ByFields.of(FieldAccessDescriptor.withFieldNames(fieldNames));
  }

  /** Same as {@link #byFieldNames(String...)}. */
  public static <T> ByFields<T> byFieldNames(Iterable<String> fieldNames) {
    return ByFields.of(FieldAccessDescriptor.withFieldNames(fieldNames));
  }

  /**
   * Returns a transform that groups all elements in the input {@link PCollection} keyed by the list
   * of fields specified. The output of this transform will have a key field of type {@link Row}
   * containing the specified extracted fields. It will also have a value field of type {@link Row}
   * containing the specified extracted fields. The returned transform contains further builder
   * methods to control how the grouping is done.
   */
  public static <T> ByFields<T> byFieldIds(Integer... fieldIds) {
    return ByFields.of(FieldAccessDescriptor.withFieldIds(fieldIds));
  }

  /** Same as {@link #byFieldIds(Integer...)}. */
  public static <T> ByFields<T> byFieldIds(Iterable<Integer> fieldIds) {
    return ByFields.of(FieldAccessDescriptor.withFieldIds(fieldIds));
  }

  /**
   * Returns a transform that groups all elements in the input {@link PCollection} keyed by the
   * fields specified. The output of this transform will have a key field of type {@link Row}
   * containing the specified extracted fields. It will also have a value field of type {@link Row}
   * containing the specified extracted fields. The returned transform contains further builder
   * methods to control how the grouping is done.
   */
  public static <T> ByFields<T> byFieldAccessDescriptor(FieldAccessDescriptor fieldAccess) {
    return ByFields.of(fieldAccess);
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
          SchemaAggregateFn.create()
              .aggregateFields(
                  FieldAccessDescriptor.withFieldNames(inputFieldName),
                  false,
                  fn,
                  outputFieldName));
    }

    public <CombineInputT, AccumT, CombineOutputT>
        CombineFieldsGlobally<InputT> aggregateFieldBaseValue(
            String inputFieldName,
            CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
            String outputFieldName) {
      return new CombineFieldsGlobally<>(
          SchemaAggregateFn.create()
              .aggregateFields(
                  FieldAccessDescriptor.withFieldNames(inputFieldName), true, fn, outputFieldName));
    }

    /** The same as {@link #aggregateField} but using field id. */
    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsGlobally<InputT> aggregateField(
        int inputFieldId,
        CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
        String outputFieldName) {
      return new CombineFieldsGlobally<>(
          SchemaAggregateFn.create()
              .aggregateFields(
                  FieldAccessDescriptor.withFieldIds(inputFieldId), false, fn, outputFieldName));
    }

    public <CombineInputT, AccumT, CombineOutputT>
        CombineFieldsGlobally<InputT> aggregateFieldBaseValue(
            int inputFieldId,
            CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
            String outputFieldName) {
      return new CombineFieldsGlobally<>(
          SchemaAggregateFn.create()
              .aggregateFields(
                  FieldAccessDescriptor.withFieldIds(inputFieldId), true, fn, outputFieldName));
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
          SchemaAggregateFn.create()
              .aggregateFields(
                  FieldAccessDescriptor.withFieldNames(inputFieldName), false, fn, outputField));
    }

    public <CombineInputT, AccumT, CombineOutputT>
        CombineFieldsGlobally<InputT> aggregateFieldBaseValue(
            String inputFieldName,
            CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
            Field outputField) {
      return new CombineFieldsGlobally<>(
          SchemaAggregateFn.create()
              .aggregateFields(
                  FieldAccessDescriptor.withFieldNames(inputFieldName), true, fn, outputField));
    }

    /** The same as {@link #aggregateField} but using field id. */
    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsGlobally<InputT> aggregateField(
        int inputFielId, CombineFn<CombineInputT, AccumT, CombineOutputT> fn, Field outputField) {
      return new CombineFieldsGlobally<>(
          SchemaAggregateFn.create()
              .aggregateFields(
                  FieldAccessDescriptor.withFieldIds(inputFielId), false, fn, outputField));
    }

    public <CombineInputT, AccumT, CombineOutputT>
        CombineFieldsGlobally<InputT> aggregateFieldBaseValue(
            int inputFielId,
            CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
            Field outputField) {
      return new CombineFieldsGlobally<>(
          SchemaAggregateFn.create()
              .aggregateFields(
                  FieldAccessDescriptor.withFieldIds(inputFielId), true, fn, outputField));
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
          SchemaAggregateFn.create()
              .aggregateFields(fieldsToAggregate, false, fn, outputFieldName));
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
          SchemaAggregateFn.create().aggregateFields(fieldsToAggregate, false, fn, outputField));
    }

    @Override
    public PCollection<Iterable<InputT>> expand(PCollection<InputT> input) {
      return input
          .apply("addNullKey", WithKeys.of((Void) null))
          .apply("group", GroupByKey.create())
          .apply("extractValues", Values.create());
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
      return input.apply("globalCombine", Combine.globally(combineFn));
    }
  }

  /**
   * a {@link PTransform} that does a global combine using an aggregation built up by calls to
   * aggregateField and aggregateFields. The output of this transform will have a schema that is
   * determined by the output types of all the composed combiners.
   */
  public static class CombineFieldsGlobally<InputT>
      extends PTransform<PCollection<InputT>, PCollection<Row>> {
    private final SchemaAggregateFn.Inner schemaAggregateFn;

    CombineFieldsGlobally(SchemaAggregateFn.Inner schemaAggregateFn) {
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
              FieldAccessDescriptor.withFieldNames(inputFieldName), false, fn, outputFieldName));
    }

    public <CombineInputT, AccumT, CombineOutputT>
        CombineFieldsGlobally<InputT> aggregateFieldBaseValue(
            String inputFieldName,
            CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
            String outputFieldName) {
      return new CombineFieldsGlobally<>(
          schemaAggregateFn.aggregateFields(
              FieldAccessDescriptor.withFieldNames(inputFieldName), true, fn, outputFieldName));
    }

    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsGlobally<InputT> aggregateField(
        int inputFieldId,
        CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
        String outputFieldName) {
      return new CombineFieldsGlobally<>(
          schemaAggregateFn.aggregateFields(
              FieldAccessDescriptor.withFieldIds(inputFieldId), false, fn, outputFieldName));
    }

    public <CombineInputT, AccumT, CombineOutputT>
        CombineFieldsGlobally<InputT> aggregateFieldBaseValue(
            int inputFieldId,
            CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
            String outputFieldName) {
      return new CombineFieldsGlobally<>(
          schemaAggregateFn.aggregateFields(
              FieldAccessDescriptor.withFieldIds(inputFieldId), true, fn, outputFieldName));
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
              FieldAccessDescriptor.withFieldNames(inputFieldName), false, fn, outputField));
    }

    public <CombineInputT, AccumT, CombineOutputT>
        CombineFieldsGlobally<InputT> aggregateFieldBaseValue(
            String inputFieldName,
            CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
            Field outputField) {
      return new CombineFieldsGlobally<>(
          schemaAggregateFn.aggregateFields(
              FieldAccessDescriptor.withFieldNames(inputFieldName), true, fn, outputField));
    }

    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsGlobally<InputT> aggregateField(
        int inputFieldId, CombineFn<CombineInputT, AccumT, CombineOutputT> fn, Field outputField) {
      return new CombineFieldsGlobally<>(
          schemaAggregateFn.aggregateFields(
              FieldAccessDescriptor.withFieldIds(inputFieldId), false, fn, outputField));
    }

    public <CombineInputT, AccumT, CombineOutputT>
        CombineFieldsGlobally<InputT> aggregateFieldBaseValue(
            int inputFieldId,
            CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
            Field outputField) {
      return new CombineFieldsGlobally<>(
          schemaAggregateFn.aggregateFields(
              FieldAccessDescriptor.withFieldIds(inputFieldId), true, fn, outputField));
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
          schemaAggregateFn.aggregateFields(fieldAccessDescriptor, false, fn, outputFieldName));
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
          schemaAggregateFn.aggregateFields(fieldAccessDescriptor, false, fn, outputField));
    }

    @Override
    public PCollection<Row> expand(PCollection<InputT> input) {
      SchemaAggregateFn.Inner fn = schemaAggregateFn.withSchema(input.getSchema());
      return input
          .apply("toRows", Convert.toRows())
          .apply("Global Combine", Combine.globally(fn))
          .setRowSchema(fn.getOutputSchema());
    }
  }

  /**
   * a {@link PTransform} that groups schema elements based on the given fields.
   *
   * <p>The output of this transform will have a key field of type {@link Row} containing the
   * specified extracted fields. It will also have a value field of type {@link Row} containing the
   * specified extracted fields.
   */
  @AutoValue
  public abstract static class ByFields<InputT>
      extends PTransform<PCollection<InputT>, PCollection<Row>> {
    abstract FieldAccessDescriptor getFieldAccessDescriptor();

    abstract String getKeyField();

    abstract String getValueField();

    abstract Builder<InputT> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<InputT> {
      abstract Builder<InputT> setFieldAccessDescriptor(
          FieldAccessDescriptor fieldAccessDescriptor);

      abstract Builder<InputT> setKeyField(String keyField);

      abstract Builder<InputT> setValueField(String valueField);

      abstract ByFields<InputT> build();
    }

    class ToKv extends PTransform<PCollection<InputT>, PCollection<KV<Row, Iterable<Row>>>> {
      private RowSelector rowSelector;

      @Override
      public PCollection<KV<Row, Iterable<Row>>> expand(PCollection<InputT> input) {
        Schema schema = input.getSchema();
        FieldAccessDescriptor resolved = getFieldAccessDescriptor().resolve(schema);
        rowSelector = new RowSelectorContainer(schema, resolved, true);
        Schema keySchema = getKeySchema(schema);

        return input
            .apply("toRow", Convert.toRows())
            .apply(
                "selectKeys",
                WithKeys.of((Row e) -> rowSelector.select(e)).withKeyType(TypeDescriptors.rows()))
            .setCoder(KvCoder.of(SchemaCoder.of(keySchema), SchemaCoder.of(schema)))
            .apply("GroupByKey", GroupByKey.create());
      }
    }

    public ToKv getToKvs() {
      return new ToKv();
    }

    private static <InputT> ByFields<InputT> of(FieldAccessDescriptor fieldAccessDescriptor) {
      return new AutoValue_Group_ByFields.Builder<InputT>()
          .setFieldAccessDescriptor(fieldAccessDescriptor)
          .setKeyField("key")
          .setValueField("value")
          .build();
    }

    public ByFields<InputT> withKeyField(String keyField) {
      return toBuilder().setKeyField(keyField).build();
    }

    public ByFields<InputT> withValueField(String valueField) {
      return toBuilder().setValueField(valueField).build();
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
      return CombineFieldsByFields.of(
          this,
          SchemaAggregateFn.create()
              .aggregateFields(
                  FieldAccessDescriptor.withFieldNames(inputFieldName), false, fn, outputFieldName),
          getKeyField(),
          getValueField());
    }

    public <CombineInputT, AccumT, CombineOutputT>
        CombineFieldsByFields<InputT> aggregateFieldBaseValue(
            String inputFieldName,
            CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
            String outputFieldName) {
      return CombineFieldsByFields.of(
          this,
          SchemaAggregateFn.create()
              .aggregateFields(
                  FieldAccessDescriptor.withFieldNames(inputFieldName), true, fn, outputFieldName),
          getKeyField(),
          getValueField());
    }

    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsByFields<InputT> aggregateField(
        int inputFieldId,
        CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
        String outputFieldName) {
      return CombineFieldsByFields.of(
          this,
          SchemaAggregateFn.create()
              .aggregateFields(
                  FieldAccessDescriptor.withFieldIds(inputFieldId), false, fn, outputFieldName),
          getKeyField(),
          getValueField());
    }

    public <CombineInputT, AccumT, CombineOutputT>
        CombineFieldsByFields<InputT> aggregateFieldBaseValue(
            int inputFieldId,
            CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
            String outputFieldName) {
      return CombineFieldsByFields.of(
          this,
          SchemaAggregateFn.create()
              .aggregateFields(
                  FieldAccessDescriptor.withFieldIds(inputFieldId), true, fn, outputFieldName),
          getKeyField(),
          getValueField());
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
      return CombineFieldsByFields.of(
          this,
          SchemaAggregateFn.create()
              .aggregateFields(
                  FieldAccessDescriptor.withFieldNames(inputFieldName), false, fn, outputField),
          getKeyField(),
          getValueField());
    }

    public <CombineInputT, AccumT, CombineOutputT>
        CombineFieldsByFields<InputT> aggregateFieldBaseValue(
            String inputFieldName,
            CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
            Field outputField) {
      return CombineFieldsByFields.of(
          this,
          SchemaAggregateFn.create()
              .aggregateFields(
                  FieldAccessDescriptor.withFieldNames(inputFieldName), true, fn, outputField),
          getKeyField(),
          getValueField());
    }

    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsByFields<InputT> aggregateField(
        int inputFieldId, CombineFn<CombineInputT, AccumT, CombineOutputT> fn, Field outputField) {
      return CombineFieldsByFields.of(
          this,
          SchemaAggregateFn.create()
              .aggregateFields(
                  FieldAccessDescriptor.withFieldIds(inputFieldId), false, fn, outputField),
          getKeyField(),
          getValueField());
    }

    public <CombineInputT, AccumT, CombineOutputT>
        CombineFieldsByFields<InputT> aggregateFieldBaseValue(
            int inputFieldId,
            CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
            Field outputField) {
      return CombineFieldsByFields.of(
          this,
          SchemaAggregateFn.create()
              .aggregateFields(
                  FieldAccessDescriptor.withFieldIds(inputFieldId), true, fn, outputField),
          getKeyField(),
          getValueField());
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
      return CombineFieldsByFields.of(
          this,
          SchemaAggregateFn.create().aggregateFields(fieldsToAggregate, false, fn, outputFieldName),
          getKeyField(),
          getValueField());
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
      return CombineFieldsByFields.of(
          this,
          SchemaAggregateFn.create().aggregateFields(fieldsToAggregate, false, fn, outputField),
          getKeyField(),
          getValueField());
    }

    Schema getKeySchema(Schema inputSchema) {
      FieldAccessDescriptor resolved = getFieldAccessDescriptor().resolve(inputSchema);
      return SelectHelpers.getOutputSchema(inputSchema, resolved);
    }

    @Override
    public PCollection<Row> expand(PCollection<InputT> input) {
      Schema schema = input.getSchema();
      Schema keySchema = getKeySchema(schema);
      Schema outputSchema =
          Schema.builder()
              .addRowField(getKeyField(), keySchema)
              .addIterableField(getValueField(), FieldType.row(schema))
              .build();

      return input
          .apply("ToKvs", getToKvs())
          .apply(
              "ToRow",
              ParDo.of(
                  new DoFn<KV<Row, Iterable<Row>>, Row>() {
                    @ProcessElement
                    public void process(@Element KV<Row, Iterable<Row>> e, OutputReceiver<Row> o) {
                      o.output(
                          Row.withSchema(outputSchema)
                              .attachValues(Lists.newArrayList(e.getKey(), e.getValue())));
                    }
                  }))
          .setRowSchema(outputSchema);
    }
  }

  /**
   * a {@link PTransform} that does a per-key combine using an aggregation built up by calls to
   * aggregateField and aggregateFields. The output of this transform will have a schema that is
   * determined by the output types of all the composed combiners.
   */
  @AutoValue
  public abstract static class CombineFieldsByFields<InputT>
      extends PTransform<PCollection<InputT>, PCollection<Row>> {
    abstract ByFields<InputT> getByFields();

    abstract SchemaAggregateFn.Inner getSchemaAggregateFn();

    abstract String getKeyField();

    abstract String getValueField();

    abstract Builder<InputT> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<InputT> {
      abstract Builder<InputT> setByFields(ByFields<InputT> byFields);

      abstract Builder<InputT> setSchemaAggregateFn(SchemaAggregateFn.Inner schemaAggregateFn);

      abstract Builder<InputT> setKeyField(String keyField);

      abstract Builder<InputT> setValueField(String valueField);

      abstract CombineFieldsByFields<InputT> build();
    }

    static <InputT> CombineFieldsByFields<InputT> of(
        ByFields<InputT> byFields,
        SchemaAggregateFn.Inner schemaAggregateFn,
        String keyField,
        String valueField) {
      return new AutoValue_Group_CombineFieldsByFields.Builder<InputT>()
          .setByFields(byFields)
          .setSchemaAggregateFn(schemaAggregateFn)
          .setKeyField(keyField)
          .setValueField(valueField)
          .build();
    }

    /** Set the name of the key field in the resulting schema. */
    public CombineFieldsByFields<InputT> withKeyField(String keyField) {
      return toBuilder().setKeyField(keyField).build();
    }

    /** Set the name of the value field in the resulting schema. */
    public CombineFieldsByFields<InputT> witValueField(String valueField) {
      return toBuilder().setValueField(valueField).build();
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
      return toBuilder()
          .setSchemaAggregateFn(
              getSchemaAggregateFn()
                  .aggregateFields(
                      FieldAccessDescriptor.withFieldNames(inputFieldName),
                      false,
                      fn,
                      outputFieldName))
          .build();
    }

    public <CombineInputT, AccumT, CombineOutputT>
        CombineFieldsByFields<InputT> aggregateFieldBaseValue(
            String inputFieldName,
            CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
            String outputFieldName) {
      return toBuilder()
          .setSchemaAggregateFn(
              getSchemaAggregateFn()
                  .aggregateFields(
                      FieldAccessDescriptor.withFieldNames(inputFieldName),
                      true,
                      fn,
                      outputFieldName))
          .build();
    }

    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsByFields<InputT> aggregateField(
        int inputFieldId,
        CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
        String outputFieldName) {
      return toBuilder()
          .setSchemaAggregateFn(
              getSchemaAggregateFn()
                  .aggregateFields(
                      FieldAccessDescriptor.withFieldIds(inputFieldId), false, fn, outputFieldName))
          .build();
    }

    public <CombineInputT, AccumT, CombineOutputT>
        CombineFieldsByFields<InputT> aggregateFieldBaseValue(
            int inputFieldId,
            CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
            String outputFieldName) {
      return toBuilder()
          .setSchemaAggregateFn(
              getSchemaAggregateFn()
                  .aggregateFields(
                      FieldAccessDescriptor.withFieldIds(inputFieldId), true, fn, outputFieldName))
          .build();
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
      return toBuilder()
          .setSchemaAggregateFn(
              getSchemaAggregateFn()
                  .aggregateFields(
                      FieldAccessDescriptor.withFieldNames(inputFieldName), false, fn, outputField))
          .build();
    }

    public <CombineInputT, AccumT, CombineOutputT>
        CombineFieldsByFields<InputT> aggregateFieldBaseValue(
            String inputFieldName,
            CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
            Field outputField) {
      return toBuilder()
          .setSchemaAggregateFn(
              getSchemaAggregateFn()
                  .aggregateFields(
                      FieldAccessDescriptor.withFieldNames(inputFieldName), true, fn, outputField))
          .build();
    }

    public <CombineInputT, AccumT, CombineOutputT> CombineFieldsByFields<InputT> aggregateField(
        int inputFieldId, CombineFn<CombineInputT, AccumT, CombineOutputT> fn, Field outputField) {
      return toBuilder()
          .setSchemaAggregateFn(
              getSchemaAggregateFn()
                  .aggregateFields(
                      FieldAccessDescriptor.withFieldIds(inputFieldId), false, fn, outputField))
          .build();
    }

    public <CombineInputT, AccumT, CombineOutputT>
        CombineFieldsByFields<InputT> aggregateFieldBaseValue(
            int inputFieldId,
            CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
            Field outputField) {
      return toBuilder()
          .setSchemaAggregateFn(
              getSchemaAggregateFn()
                  .aggregateFields(
                      FieldAccessDescriptor.withFieldIds(inputFieldId), true, fn, outputField))
          .build();
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
      return toBuilder()
          .setSchemaAggregateFn(
              getSchemaAggregateFn().aggregateFields(fieldsToAggregate, false, fn, outputFieldName))
          .build();
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
      return toBuilder()
          .setSchemaAggregateFn(
              getSchemaAggregateFn().aggregateFields(fieldsToAggregate, false, fn, outputField))
          .build();
    }

    @Override
    public PCollection<Row> expand(PCollection<InputT> input) {
      SchemaAggregateFn.Inner fn = getSchemaAggregateFn().withSchema(input.getSchema());

      Schema keySchema = getByFields().getKeySchema(input.getSchema());
      Schema outputSchema =
          Schema.builder()
              .addRowField(getKeyField(), keySchema)
              .addRowField(getValueField(), getSchemaAggregateFn().getOutputSchema())
              .build();

      return input
          .apply("ToKvs", getByFields().getToKvs())
          .apply("Combine", Combine.groupedValues(fn))
          .apply(
              "ToRow",
              ParDo.of(
                  new DoFn<KV<Row, Row>, Row>() {
                    @ProcessElement
                    public void process(@Element KV<Row, Row> element, OutputReceiver<Row> o) {
                      o.output(
                          Row.withSchema(outputSchema)
                              .attachValues(
                                  Lists.newArrayList(element.getKey(), element.getValue())));
                    }
                  }))
          .setRowSchema(outputSchema);
    }
  }
}
