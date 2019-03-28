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
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.utils.SelectHelpers;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Maps;

/**
 * A transform that performs equijoins across multiple schema {@link PCollection}s.
 *
 * <p>This transform has similarites to {@link CoGroupByKey}, however works on PCollections that
 * have schemas. This allows users of the transform to simply specify schema fields to join on. The
 * output type of the transform is a {@code KV<Row, Row>} where the value contains one field for
 * every input PCollection and the key represents the fields that were joined on. By default the
 * cross product is not expanded, so all fields in the output row are array fields.
 *
 * <p>For example, the following demonstrates joining three PCollections on the "user" and "country"
 * fields:
 *
 * <pre>{@code PCollection<KV<Row, Row>> joined =
 *   PCollectionTuple.of("input1", input1, "input2", input2, "input3", input3)
 *     .apply(CoGroup.join(By.fieldNames("user", "country")));
 * }</pre>
 *
 * <p>In the above case, the key schema will contain the two string fields "user" and "country"; in
 * this case, the schemas for Input1, Input2, Input3 must all have fields named "user" and
 * "country". The value schema will contain three array of Row fields named "input1" "input2" and
 * "input3". The value Row contains all inputs that came in on any of the inputs for that key.
 * Standard join types (inner join, outer join, etc.) can be accomplished by expanding the cross
 * product of these arrays in various ways.
 *
 * <p>To put it in other words, the key schema is convertible to the following POJO:
 *
 * <pre>{@code @DefaultSchema(JavaFieldSchema.class)
 * public class JoinedKey {
 *   public String user;
 *   public String country;
 * }
 *
 * PCollection<JoinedKey> keys = joined
 *     .apply(Keys.create())
 *     .apply(Convert.to(JoinedKey.class));
 * }</pre>
 *
 * <p>The value schema is convertible to the following POJO:
 *
 * <pre>{@code @DefaultSchema(JavaFieldSchema.class)
 * public class JoinedValue {
 *   // The below lists contain all values from each of the three inputs that match on the given
 *   // key.
 *   public List<Input1Type> input1;
 *   public List<Input2Type> input2;
 *   public List<Input3Type> input3;
 * }
 *
 * PCollection<JoinedValue> values = joined
 *     .apply(Values.create())
 *     .apply(Convert.to(JoinedValue.class));
 * }</pre>
 *
 * <p>It's also possible to join between different fields in two inputs, as long as the types of
 * those fields match. In this case, fields must be specified for every input PCollection. For
 * example:
 *
 * <pre>{@code PCollection<KV<Row, Row>> joined
 *      = PCollectionTuple.of("input1Tag", input1, "input2Tag", input2)
 *   .apply(CoGroup
 *     .join("input1Tag", By.fieldNames("referringUser")))
 *     .join("input2Tag", By.fieldNames("user")));
 * }</pre>
 *
 * <p>Traditional (SQL) joins are cross-product joins. All rows that match the join condition are
 * combined into individual rows and returned; in fact any SQL inner joins is a subset of the
 * cross-product of two tables. This transform also supports the same functionality using the {@link
 * Inner#crossProductJoin()} method.
 *
 * <p>For example, consider the SQL join: SELECT * FROM input1 INNER JOIN input2 ON input1.user =
 * input2.user
 *
 * <p>You could express this with:
 *
 * <pre>{@code
 * PCollection<Row> joined = PCollectionTuple.of("input1", input1, "input2", input2)
 *   .apply(CoGroup.join(By.fieldNames("user")).crossProductJoin();
 * }</pre>
 *
 * <p>The schema of the output PCollection contains a nested message for each of input1 and input2.
 * Like above, you could use the {@link Convert} transform to convert it to the following POJO:
 *
 * <pre>{@code
 * {@literal @}DefaultSchema(JavaFieldSchema.class)
 * public class JoinedValue {
 *   public Input1Type input1;
 *   public Input2Type input2;
 * }
 * }</pre>
 *
 * <p>The {@link Unnest} transform can then be used to flatten all the subfields into one single
 * top-level row containing all the fields in both Input1 and Input2; this will often be combined
 * with a {@link Select} transform to select out the fields of interest, as the key fields will be
 * identical between input1 and input2.
 *
 * <p>This transform also supports outer-join semantics. By default, all input PCollections must
 * participate fully in the join, providing inner-join semantics. This means that the join will only
 * produce values for "Bob" if all inputs have values for "Bob;" if even a single input does not
 * have a value for "Bob," an inner-join will produce no value. However, if you mark that input as
 * having outer-join participation then the join will contain values for "Bob," as long as at least
 * one input has a "Bob" value; null values will be added for inputs that have no "Bob" values. To
 * continue the SQL example:
 *
 * <p>SELECT * FROM input1 LEFT OUTER JOIN input2 ON input1.user = input2.user
 *
 * <p>Is equivalent to:
 *
 * <pre>{@code
 * PCollection<Row> joined = PCollectionTuple.of("input1", input1, "input2", input2)
 *   .apply(CoGroup.join("input1", By.fieldNames("user").withOuterJoinParticipation())
 *                 .join("input2", By.fieldNames("user"))
 *                 .crossProductJoin();
 * }</pre>
 *
 * <p>SELECT * FROM input1 RIGHT OUTER JOIN input2 ON input1.user = input2.user
 *
 * <p>Is equivalent to:
 *
 * <pre>{@code
 * PCollection<Row> joined = PCollectionTuple.of("input1", input1, "input2", input2)
 *   .apply(CoGroup.join("input1", By.fieldNames("user"))
 *                 .join("input2", By.fieldNames("user").withOuterJoinParticipation())
 *                 .crossProductJoin();
 * }</pre>
 *
 * <p>and SELECT * FROM input1 FULL OUTER JOIN input2 ON input1.user = input2.user
 *
 * <p>Is equivalent to:
 *
 * <pre>{@code
 * PCollection<Row> joined = PCollectionTuple.of("input1", input1, "input2", input2)
 *   .apply(CoGroup.join("input1", By.fieldNames("user").withOuterJoinParticipation())
 *                 .join("input2", By.fieldNames("user").withOuterJoinParticipation())
 *                 .crossProductJoin();
 * }</pre>
 *
 * <p>While the above examples use two inputs to mimic SQL's left and right join semantics, the
 * {@link CoGroup} transform supports any number of inputs, and outer-join participation can be
 * specified on any subset of them.
 *
 * <p>Do note that cross-product joins while simpler and easier to program, can cause
 */
public class CoGroup {
  private static final List NULL_LIST;

  static {
    NULL_LIST = Lists.newArrayList();
    NULL_LIST.add(null);
  }

  /**
   * Defines the set of fields to extract for the join key, as well as other per-input join options.
   */
  @AutoValue
  public abstract static class By implements Serializable {
    abstract FieldAccessDescriptor getFieldAccessDescriptor();

    abstract boolean getOuterJoinParticipation();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setFieldAccessDescriptor(FieldAccessDescriptor fieldAccessDescriptor);

      abstract Builder setOuterJoinParticipation(boolean outerJoinParticipation);

      abstract By build();
    }

    /** Join by the following field names. */
    public static By fieldNames(String... fieldNames) {
      return fieldAccessDescriptor(FieldAccessDescriptor.withFieldNames(fieldNames));
    }

    /** Join by the following field ids. */
    public static By fieldIds(Integer... fieldIds) {
      return fieldAccessDescriptor(FieldAccessDescriptor.withFieldIds(fieldIds));
    }

    /** Join by the following field access descriptor. */
    public static By fieldAccessDescriptor(FieldAccessDescriptor fieldAccessDescriptor) {
      return new AutoValue_CoGroup_By.Builder()
          .setFieldAccessDescriptor(fieldAccessDescriptor)
          .setOuterJoinParticipation(false)
          .build();
    }

    /**
     * Means that this field will participate in a join even when not present, similar to SQL
     * outer-join semantics. Missing entries will be replaced by nulls.
     *
     * <p>This only affects the results of expandCrossProduct.
     */
    public By withOuterJoinParticipation() {
      return toBuilder().setOuterJoinParticipation(true).build();
    }
  }

  private static class JoinArguments implements Serializable {
    @Nullable private final By allInputsJoinArgs;
    private final Map<String, By> joinArgsMap;

    JoinArguments(@Nullable By allInputsJoinArgs) {
      this.allInputsJoinArgs = allInputsJoinArgs;
      this.joinArgsMap = Collections.emptyMap();
    }

    JoinArguments(Map<String, By> joinArgsMap) {
      this.allInputsJoinArgs = null;
      this.joinArgsMap = joinArgsMap;
    }

    JoinArguments with(String tag, By clause) {
      return new JoinArguments(
          new ImmutableMap.Builder<String, By>().putAll(joinArgsMap).put(tag, clause).build());
    }

    @Nullable
    private FieldAccessDescriptor getFieldAccessDescriptor(String tag) {
      return (allInputsJoinArgs != null)
          ? allInputsJoinArgs.getFieldAccessDescriptor()
          : joinArgsMap.get(tag).getFieldAccessDescriptor();
    }

    private boolean getOuterJoinParticipation(String tag) {
      return (allInputsJoinArgs != null)
          ? allInputsJoinArgs.getOuterJoinParticipation()
          : joinArgsMap.get(tag).getOuterJoinParticipation();
    }
  }

  /**
   * Join all input PCollections using the same args.
   *
   * <p>The same fields and other options are used in all input PCollections.
   */
  public static Inner join(By clause) {
    return new Inner(new JoinArguments(clause));
  }

  /**
   * Specify the following join arguments (including fields to join by_ for the specified
   * PCollection.
   *
   * <p>Each PCollection in the input must have args specified for the join key.
   */
  public static Inner join(String tag, By clause) {
    return new Inner(new JoinArguments(ImmutableMap.of(tag, clause)));
  }

  // Contains summary information needed for implementing the join.
  private static class JoinInformation {
    private final KeyedPCollectionTuple<Row> keyedPCollectionTuple;
    private final Schema keySchema;
    private final Map<String, Schema> componentSchemas;
    // Maps from index in sortedTags to the toRow function.
    private final Map<Integer, SerializableFunction<Object, Row>> toRows;
    private final List<String> sortedTags;
    private final Map<Integer, String> tagToKeyedTag;

    private JoinInformation(
        KeyedPCollectionTuple<Row> keyedPCollectionTuple,
        Schema keySchema,
        Map<String, Schema> componentSchemas,
        Map<Integer, SerializableFunction<Object, Row>> toRows,
        List<String> sortedTags,
        Map<Integer, String> tagToKeyedTag) {
      this.keyedPCollectionTuple = keyedPCollectionTuple;
      this.keySchema = keySchema;
      this.componentSchemas = componentSchemas;
      this.toRows = toRows;
      this.sortedTags = sortedTags;
      this.tagToKeyedTag = tagToKeyedTag;
    }

    private static JoinInformation from(
        PCollectionTuple input, Function<String, FieldAccessDescriptor> getFieldAccessDescriptor) {
      KeyedPCollectionTuple<Row> keyedPCollectionTuple =
          KeyedPCollectionTuple.empty(input.getPipeline());

      List<String> sortedTags =
          input.getAll().keySet().stream()
              .map(TupleTag::getId)
              .sorted()
              .collect(Collectors.toList());

      // Keep this in a TreeMap so that it's sorted. This way we get a deterministic output
      // schema.
      TreeMap<String, Schema> componentSchemas = Maps.newTreeMap();
      Map<Integer, SerializableFunction<Object, Row>> toRows = Maps.newHashMap();

      Map<Integer, String> tagToKeyedTag = Maps.newHashMap();
      Schema keySchema = null;
      for (Map.Entry<TupleTag<?>, PCollection<?>> entry : input.getAll().entrySet()) {
        String tag = entry.getKey().getId();
        int tagIndex = sortedTags.indexOf(tag);
        PCollection<?> pc = entry.getValue();
        Schema schema = pc.getSchema();
        componentSchemas.put(tag, schema);
        toRows.put(tagIndex, (SerializableFunction<Object, Row>) pc.getToRowFunction());
        FieldAccessDescriptor fieldAccessDescriptor = getFieldAccessDescriptor.apply(tag);
        if (fieldAccessDescriptor == null) {
          throw new IllegalStateException("No fields were set for input " + tag);
        }
        // Resolve the key schema, keeping the fields in the order specified by the user.
        // Otherwise, if different field names are specified for different PCollections, they
        // might not match up.
        // The key schema contains the field names from the first PCollection specified.
        FieldAccessDescriptor resolved =
            fieldAccessDescriptor.withOrderByFieldInsertionOrder().resolve(schema);
        Schema currentKeySchema = SelectHelpers.getOutputSchema(schema, resolved);
        if (keySchema == null) {
          keySchema = currentKeySchema;
        } else {
          if (!currentKeySchema.typesEqual(keySchema)) {
            throw new IllegalStateException("All keys must have the same schema");
          }
        }

        // Create a new tag for the output.
        TupleTag randomTag = new TupleTag<>();
        String keyedTag = tag + "_" + randomTag;
        tagToKeyedTag.put(tagIndex, keyedTag);
        PCollection<KV<Row, Row>> keyedPCollection =
            extractKey(pc, schema, keySchema, resolved, tag);
        keyedPCollectionTuple = keyedPCollectionTuple.and(keyedTag, keyedPCollection);
      }
      return new JoinInformation(
          keyedPCollectionTuple, keySchema, componentSchemas, toRows, sortedTags, tagToKeyedTag);
    }

    private static <T> PCollection<KV<Row, Row>> extractKey(
        PCollection<T> pCollection,
        Schema schema,
        Schema keySchema,
        FieldAccessDescriptor keyFields,
        String tag) {
      return pCollection
          .apply(
              "extractKey" + tag,
              ParDo.of(
                  new DoFn<T, KV<Row, Row>>() {
                    @ProcessElement
                    public void process(@Element Row row, OutputReceiver<KV<Row, Row>> o) {
                      o.output(
                          KV.of(SelectHelpers.selectRow(row, keyFields, schema, keySchema), row));
                    }
                  }))
          .setCoder(KvCoder.of(SchemaCoder.of(keySchema), SchemaCoder.of(schema)));
    }
  }

  static void verify(PCollectionTuple input, JoinArguments joinArgs) {
    if (joinArgs.allInputsJoinArgs == null) {
      // If explicit join tags were specified, then they must match the input tuple.
      Set<String> inputTags =
          input.getAll().keySet().stream().map(TupleTag::getId).collect(Collectors.toSet());
      Set<String> joinTags = joinArgs.joinArgsMap.keySet();
      if (!inputTags.equals(joinTags)) {
        throw new IllegalArgumentException(
            "The input PCollectionTuple has tags: "
                + inputTags
                + " and the join was specified for tags "
                + joinTags
                + ". These do not match.");
      }
    }
  }

  /** The implementing PTransform. */
  public static class Inner extends PTransform<PCollectionTuple, PCollection<KV<Row, Row>>> {
    private final JoinArguments joinArgs;

    private Inner() {
      this(new JoinArguments(Collections.emptyMap()));
    }

    private Inner(JoinArguments joinArgs) {
      this.joinArgs = joinArgs;
    }

    /**
     * Select the following fields for the specified PCollection with the specified join args.
     *
     * <p>Each PCollection in the input must have fields specified for the join key.
     */
    public Inner join(String tag, By clause) {
      if (joinArgs.allInputsJoinArgs != null) {
        throw new IllegalStateException("Cannot set both a global and per-tag fields.");
      }
      return new Inner(joinArgs.with(tag, clause));
    }

    /** Expand the join into individual rows, similar to SQL joins. */
    public ExpandCrossProduct crossProductJoin() {
      return new ExpandCrossProduct(joinArgs);
    }

    private Schema getOutputSchema(JoinInformation joinInformation) {
      // Construct the output schema. It contains one field for each input PCollection, of type
      // ARRAY[ROW].
      Schema.Builder joinedSchemaBuilder = Schema.builder();
      for (Map.Entry<String, Schema> entry : joinInformation.componentSchemas.entrySet()) {
        joinedSchemaBuilder.addArrayField(entry.getKey(), FieldType.row(entry.getValue()));
      }
      return joinedSchemaBuilder.build();
    }

    @Override
    public PCollection<KV<Row, Row>> expand(PCollectionTuple input) {
      verify(input, joinArgs);

      JoinInformation joinInformation =
          JoinInformation.from(input, joinArgs::getFieldAccessDescriptor);

      Schema joinedSchema = getOutputSchema(joinInformation);

      return joinInformation
          .keyedPCollectionTuple
          .apply("CoGroupByKey", CoGroupByKey.create())
          .apply(
              "ConvertToRow",
              ParDo.of(
                  new ConvertToRow(
                      joinInformation.sortedTags,
                      joinInformation.toRows,
                      joinedSchema,
                      joinInformation.tagToKeyedTag)))
          .setCoder(
              KvCoder.of(SchemaCoder.of(joinInformation.keySchema), SchemaCoder.of(joinedSchema)));
    }

    // Used by the unexpanded join to create the output rows.
    private static class ConvertToRow extends DoFn<KV<Row, CoGbkResult>, KV<Row, Row>> {
      private final List<String> sortedTags;
      private final Map<Integer, SerializableFunction<Object, Row>> toRows;
      private final Map<Integer, String> tagToKeyedTag;
      private final Schema joinedSchema;

      ConvertToRow(
          List<String> sortedTags,
          Map<Integer, SerializableFunction<Object, Row>> toRows,
          Schema joinedSchema,
          Map<Integer, String> tagToKeyedTag) {
        this.sortedTags = sortedTags;
        this.toRows = toRows;
        this.joinedSchema = joinedSchema;
        this.tagToKeyedTag = tagToKeyedTag;
      }

      @ProcessElement
      public void process(@Element KV<Row, CoGbkResult> kv, OutputReceiver<KV<Row, Row>> o) {
        Row key = kv.getKey();
        CoGbkResult result = kv.getValue();
        List<Object> fields = Lists.newArrayListWithCapacity(sortedTags.size());
        for (int i = 0; i < sortedTags.size(); ++i) {
          String tag = sortedTags.get(i);
          // TODO: This forces the entire join to materialize in memory. We should create a
          // lazy Row interface on top of the iterable returned by CoGbkResult. This will
          // allow the data to be streamed in. Tracked in [BEAM-6756].
          SerializableFunction<Object, Row> toRow = toRows.get(i);
          String tupleTag = tagToKeyedTag.get(i);
          List<Row> joined = Lists.newArrayList();
          for (Object item : result.getAll(tupleTag)) {
            joined.add(toRow.apply(item));
          }
          fields.add(joined);
        }
        o.output(KV.of(key, Row.withSchema(joinedSchema).addValues(fields).build()));
      }
    }
  }

  /** A {@link PTransform} that calculates the cross-product join. */
  public static class ExpandCrossProduct extends PTransform<PCollectionTuple, PCollection<Row>> {
    private final JoinArguments joinArgs;

    ExpandCrossProduct(JoinArguments joinArgs) {
      this.joinArgs = joinArgs;
    }

    /**
     * Select the following fields for the specified PCollection with the specified join args.
     *
     * <p>Each PCollection in the input must have fields specified for the join key.
     */
    public ExpandCrossProduct join(String tag, By clause) {
      if (joinArgs.allInputsJoinArgs != null) {
        throw new IllegalStateException("Cannot set both a global and per-tag fields.");
      }
      return new ExpandCrossProduct(joinArgs.with(tag, clause));
    }

    private Schema getOutputSchema(JoinInformation joinInformation) {
      // Construct the output schema. It contains one field for each input PCollection, of type
      // ROW. If a field supports outer-join semantics, then that field will be nullable in the
      // schema.
      Schema.Builder joinedSchemaBuilder = Schema.builder();
      for (Map.Entry<String, Schema> entry : joinInformation.componentSchemas.entrySet()) {
        FieldType fieldType = FieldType.row(entry.getValue());
        if (joinArgs.getOuterJoinParticipation(entry.getKey())) {
          fieldType = fieldType.withNullable(true);
        }
        joinedSchemaBuilder.addField(entry.getKey(), fieldType);
      }
      return joinedSchemaBuilder.build();
    }

    @Override
    public PCollection<Row> expand(PCollectionTuple input) {
      verify(input, joinArgs);

      JoinInformation joinInformation =
          JoinInformation.from(input, joinArgs::getFieldAccessDescriptor);

      Schema joinedSchema = getOutputSchema(joinInformation);

      return joinInformation
          .keyedPCollectionTuple
          .apply("CoGroupByKey", CoGroupByKey.create())
          .apply("Values", Values.create())
          .apply(
              "ExpandToRow",
              ParDo.of(
                  new ExpandToRows(
                      joinInformation.sortedTags,
                      joinInformation.toRows,
                      joinedSchema,
                      joinInformation.tagToKeyedTag)))
          .setRowSchema(joinedSchema);
    }

    /** A DoFn that expands the result of a CoGroupByKey into the cross product. */
    private class ExpandToRows extends DoFn<CoGbkResult, Row> {
      private final List<String> sortedTags;
      private final Map<Integer, SerializableFunction<Object, Row>> toRows;
      private final Schema outputSchema;
      private final Map<Integer, String> tagToKeyedTag;

      public ExpandToRows(
          List<String> sortedTags,
          Map<Integer, SerializableFunction<Object, Row>> toRows,
          Schema outputSchema,
          Map<Integer, String> tagToKeyedTag) {
        this.sortedTags = sortedTags;
        this.toRows = toRows;
        this.outputSchema = outputSchema;
        this.tagToKeyedTag = tagToKeyedTag;
      }

      @ProcessElement
      public void process(@Element CoGbkResult gbkResult, OutputReceiver<Row> o) {
        List<Iterable> allIterables = extractIterables(gbkResult);
        List<Row> accumulatedRows = Lists.newArrayListWithCapacity(sortedTags.size());
        crossProduct(0, accumulatedRows, allIterables, o);
      }

      private List<Iterable> extractIterables(CoGbkResult gbkResult) {
        List<Iterable> iterables = Lists.newArrayListWithCapacity(sortedTags.size());
        for (int i = 0; i < sortedTags.size(); ++i) {
          String tag = sortedTags.get(i);
          Iterable items = gbkResult.getAll(tagToKeyedTag.get(i));
          if (!items.iterator().hasNext() && joinArgs.getOuterJoinParticipation(tag)) {
            // If this tag has outer-join participation, then empty should participate as a
            // single null.
            items = () -> NULL_LIST.iterator();
          }
          iterables.add(items);
        }
        return iterables;
      }

      private void crossProduct(
          int tagIndex,
          List<Row> accumulatedRows,
          List<Iterable> iterables,
          OutputReceiver<Row> o) {
        if (tagIndex >= sortedTags.size()) {
          return;
        }

        SerializableFunction<Object, Row> toRow = toRows.get(tagIndex);
        for (Object item : iterables.get(tagIndex)) {
          // For every item that joined for the current input, and recurse down to calculate the
          // list of expanded records.
          Row row = toRow.apply(item);
          crossProductHelper(tagIndex, accumulatedRows, row, iterables, o);
        }
      }

      private void crossProductHelper(
          int tagIndex,
          List<Row> accumulatedRows,
          Row newRow,
          List<Iterable> iterables,
          OutputReceiver<Row> o) {
        boolean atBottom = tagIndex == sortedTags.size() - 1;
        accumulatedRows.add(newRow);
        if (atBottom) {
          // Bottom of recursive call, so output the row we've accumulated.
          o.output(buildOutputRow(accumulatedRows));
        } else {
          crossProduct(tagIndex + 1, accumulatedRows, iterables, o);
        }
        accumulatedRows.remove(accumulatedRows.size() - 1);
      }

      private Row buildOutputRow(List rows) {
        return Row.withSchema(outputSchema).addValues(rows).build();
      }
    }
  }
}
