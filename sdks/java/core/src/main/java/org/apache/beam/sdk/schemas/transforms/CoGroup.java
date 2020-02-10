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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
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
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;

/**
 * A transform that performs equijoins across multiple schema {@link PCollection}s.
 *
 * <p>This transform has similarities to {@link CoGroupByKey}, however works on PCollections that
 * have schemas. This allows users of the transform to simply specify schema fields to join on. The
 * output type of the transform is {@code Row} that contains one row field for the key and an ITERABLE
 * field for each input containing the rows that joined on that key; by default the cross product is
 * not expanded, but the cross product can be optionally expanded. By default the key field is named
 * "key" (the name can be overridden using withKeyField) and has index 0. The tags in the
 * PCollectionTuple control the names of the value fields in the Row.
 *
 * <p>For example, the following demonstrates joining three PCollections on the "user" and "country"
 * fields:
 *
 * <pre>{@code PCollection<Row> joined =
 *   PCollectionTuple.of("input1", input1, "input2", input2, "input3", input3)
 *     .apply(CoGroup.join(By.fieldNames("user", "country")));
 * }</pre>
 *
 * <p>In the above case, the key schema will contain the two string fields "user" and "country"; in
 * this case, the schemas for Input1, Input2, Input3 must all have fields named "user" and
 * "country". The remainder of the Row will contain three iterable of Row fields named "input1"
 * "input2" and "input3". This contains all inputs that came in on any of the inputs for that key.
 * Standard join types (inner join, outer join, etc.) can be accomplished by expanding the cross
 * product of these iterables in various ways.
 *
 * <p>To put it in other words, the key schema is convertible to the following POJO:
 *
 * <pre>{@code @DefaultSchema(JavaFieldSchema.class)
 * public class JoinedKey {
 *   public String user;
 *   public String country;
 * }</pre>
 *
 *  <p>The value schema is convertible to the following POJO:
 *
 *  <pre>{@code @DefaultSchema(JavaFieldSchema.class)
 *  public class JoinedValue {
 *    public JoinedKey key;
 *    // The below lists contain all values from each of the three inputs that match on the given
 *    // key.
 *    public Iterable<Input1Type> input1;
 *    public Iterable<Input2Type> input2;
 *    public Iterable<Input3Type> input3;
 *  }
 *
 * PCollection<JoinedValue> values = joined.apply(Convert.to(JoinedValue.class));
 *
 * PCollection<JoinedKey> keys = values
 *     .apply(Select.fieldNames("key"))
 *     .apply(Convert.to(JoinedKey.class));
 * }</pre>
 *
 *
 *
 * <p>It's also possible to join between different fields in two inputs, as long as the types of
 * those fields match. In this case, fields must be specified for every input PCollection. For
 * example:
 *
 * <pre>{@code PCollection<Row> joined
 *      = PCollectionTuple.of("input1Tag", input1, "input2Tag", input2)
 *   .apply(CoGroup
 *     .join("input1Tag", By.fieldNames("referringUser")))
 *     .join("input2Tag", By.fieldNames("user")));
 * }</pre>
 *
 * <p>Traditional (SQL) joins are cross-product joins. All rows that match the join condition are
 * combined into individual rows and returned; in fact any SQL inner joins is a subset of the
 * cross-product of two tables. This transform also supports the same functionality using the {@link
 * Impl#crossProductJoin()} method.
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
 * <p> {@link Select#flattenedSchema()}  can then be used to flatten all the subfields into one single
 * top-level row containing all the fields in both Input1 and Input2; this will often be combined
 * with a {@link Select} transform to select out the fields of interest, as the key fields will be
 * identical between input1 and input2.
 *
 * <p>This transform also supports outer-join semantics. By default, all input PCollections must
 * participate fully in the join, providing inner-join semantics. This means that the join will only
 * produce values for "Bob" if all inputs have values for "Bob;" if even a single input does not
 * have a value for "Bob," an inner-join will produce no value. However, if you mark that input as
 * having optional participation then the join will contain values for "Bob," as long as at least
 * one input has a "Bob" value; null values will be added for inputs that have no "Bob" values. To
 * continue the SQL example:
 *
 * <p>SELECT * FROM input1 LEFT OUTER JOIN input2 ON input1.user = input2.user
 *
 * <p>Is equivalent to:
 *
 * <pre>{@code
 * PCollection<Row> joined = PCollectionTuple.of("input1", input1, "input2", input2)
 *   .apply(CoGroup.join("input1", By.fieldNames("user").withOptionalParticipation())
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
 *                 .join("input2", By.fieldNames("user").withOptionalParticipation())
 *                 .crossProductJoin();
 * }</pre>
 *
 * <p>and SELECT * FROM input1 FULL OUTER JOIN input2 ON input1.user = input2.user
 *
 * <p>Is equivalent to:
 *
 * <pre>{@code
 * PCollection<Row> joined = PCollectionTuple.of("input1", input1, "input2", input2)
 *   .apply(CoGroup.join("input1", By.fieldNames("user").withOptionalParticipation())
 *                 .join("input2", By.fieldNames("user").withOptionalParticipation())
 *                 .crossProductJoin();
 * }</pre>
 *
 * <p>While the above examples use two inputs to mimic SQL's left and right join semantics, the
 * {@link CoGroup} transform supports any number of inputs, and optional participation can be
 * specified on any subset of them.
 *
 * <p>Do note that cross-product joins while simpler and easier to program, can cause performance problems.
 */
@Experimental(Kind.SCHEMAS)
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

    abstract boolean getOptionalParticipation();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setFieldAccessDescriptor(FieldAccessDescriptor fieldAccessDescriptor);

      abstract Builder setOptionalParticipation(boolean optionalParticipation);

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
          .setOptionalParticipation(false)
          .build();
    }

    /**
     * Means that this field will participate in a join even when not present, similar to SQL
     * outer-join semantics. Missing entries will be replaced by nulls.
     *
     * <p>This only affects the results of expandCrossProduct.
     */
    public By withOptionalParticipation() {
      return toBuilder().setOptionalParticipation(true).build();
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

    private boolean getOptionalParticipation(String tag) {
      return (allInputsJoinArgs != null)
          ? allInputsJoinArgs.getOptionalParticipation()
          : joinArgsMap.get(tag).getOptionalParticipation();
    }
  }

  /**
   * Join all input PCollections using the same args.
   *
   * <p>The same fields and other options are used in all input PCollections.
   */
  public static Impl join(By clause) {
    return new Impl(new JoinArguments(clause));
  }

  /**
   * Specify the following join arguments (including fields to join by_ for the specified
   * PCollection.
   *
   * <p>Each PCollection in the input must have args specified for the join key.
   */
  public static Impl join(String tag, By clause) {
    return new Impl(new JoinArguments(ImmutableMap.of(tag, clause)));
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
  public static class Impl extends PTransform<PCollectionTuple, PCollection<Row>> {
    private final JoinArguments joinArgs;
    private final String keyFieldName;

    private Impl() {
      this(new JoinArguments(Collections.emptyMap()));
    }

    private Impl(JoinArguments joinArgs) {
      this(joinArgs, "key");
    }

    private Impl(JoinArguments joinArgs, String keyFieldName) {
      this.joinArgs = joinArgs;
      this.keyFieldName = keyFieldName;
    }

    public Impl withKeyField(String keyFieldName) {
      return new Impl(joinArgs, keyFieldName);
    }

    /**
     * Select the following fields for the specified PCollection with the specified join args.
     *
     * <p>Each PCollection in the input must have fields specified for the join key.
     */
    public Impl join(String tag, By clause) {
      if (joinArgs.allInputsJoinArgs != null) {
        throw new IllegalStateException("Cannot set both a global and per-tag fields.");
      }
      return new Impl(joinArgs.with(tag, clause), keyFieldName);
    }

    /** Expand the join into individual rows, similar to SQL joins. */
    public ExpandCrossProduct crossProductJoin() {
      return new ExpandCrossProduct(joinArgs);
    }

    @Override
    public PCollection<Row> expand(PCollectionTuple input) {
      verify(input, joinArgs);

      JoinInformation joinInformation =
          JoinInformation.from(input, joinArgs::getFieldAccessDescriptor);
      ConvertToRow convertToRow = new ConvertToRow(joinInformation, keyFieldName);
      return joinInformation
          .keyedPCollectionTuple
          .apply("CoGroupByKey", CoGroupByKey.create())
          .apply("ConvertToRow", ParDo.of(convertToRow))
          .setRowSchema(convertToRow.getOutputSchema());
    }

    // Used by the unexpanded join to create the output rows.
    private static class ConvertToRow extends DoFn<KV<Row, CoGbkResult>, Row> {
      private final List<String> sortedTags;
      private final Map<Integer, String> tagToKeyedTag;
      private final Map<Integer, SerializableFunction<Object, Row>> toRows;

      private final Schema outputSchema;

      ConvertToRow(JoinInformation joinInformation, String keyFieldName) {
        this.sortedTags = joinInformation.sortedTags;
        this.tagToKeyedTag = joinInformation.tagToKeyedTag;
        this.toRows = joinInformation.toRows;
        Schema.Builder schemaBuilder =
            Schema.builder().addRowField(keyFieldName, joinInformation.keySchema);
        for (Map.Entry<String, Schema> entry : joinInformation.componentSchemas.entrySet()) {
          schemaBuilder.addIterableField(entry.getKey(), FieldType.row(entry.getValue()));
        }
        outputSchema = schemaBuilder.build();
      }

      Schema getOutputSchema() {
        return outputSchema;
      }

      /** Lazy iterable that wraps the result returned from CoGroupByKey. */
      static final class Result implements Iterable<Row> {
        private Iterable<Row> coGbkIterable;
        private SerializableFunction<Object, Row> toRow;

        Result(Iterable<Row> coGbkIterable, SerializableFunction<Object, Row> toRow) {
          this.coGbkIterable = coGbkIterable;
          this.toRow = toRow;
        }

        @Override
        public Iterator<Row> iterator() {
          return new Iterator<Row>() {
            private Iterator<Row> coGbkIterator = coGbkIterable.iterator();

            @Override
            public boolean hasNext() {
              return coGbkIterator.hasNext();
            }

            @Override
            public Row next() {
              return toRow.apply(coGbkIterator.next());
            }
          };
        }
      }

      @ProcessElement
      public void process(@Element KV<Row, CoGbkResult> kv, OutputReceiver<Row> o) {
        Row key = kv.getKey();
        CoGbkResult result = kv.getValue();
        List<Object> fields = Lists.newArrayListWithCapacity(sortedTags.size() + 1);
        fields.add(key);
        for (int i = 0; i < sortedTags.size(); ++i) {
          String tupleTag = tagToKeyedTag.get(i);
          SerializableFunction<Object, Row> toRow = toRows.get(i);
          fields.add(new Result(result.getAll(tupleTag), toRow));
        }
        Row row = Row.withSchema(outputSchema).attachValues(fields).build();
        o.output(row);
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
      // ROW. If a field has optional participation, then that field will be nullable in the
      // schema.
      Schema.Builder joinedSchemaBuilder = Schema.builder();
      for (Map.Entry<String, Schema> entry : joinInformation.componentSchemas.entrySet()) {
        FieldType fieldType = FieldType.row(entry.getValue());
        if (joinArgs.getOptionalParticipation(entry.getKey())) {
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
          if (!items.iterator().hasNext() && joinArgs.getOptionalParticipation(tag)) {
            // If this tag has optional participation, then empty should participate as a
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
        return Row.withSchema(outputSchema).attachValues(Lists.newArrayList(rows)).build();
      }
    }
  }
}
