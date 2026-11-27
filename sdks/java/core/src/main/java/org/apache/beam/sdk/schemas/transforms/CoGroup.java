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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaUtils;
import org.apache.beam.sdk.schemas.transforms.CoGroup.ConvertCoGbkResult.ConvertType;
import org.apache.beam.sdk.schemas.utils.RowSelector;
import org.apache.beam.sdk.schemas.utils.SelectHelpers;
import org.apache.beam.sdk.schemas.utils.SelectHelpers.RowSelectorContainer;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.Nullable;

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
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class CoGroup {
  private static final List<@Nullable Row> NULL_LIST = Collections.singletonList(null);

  private static boolean isEmpty(Iterable<Row> rows) {
    if (rows instanceof Collection) {
      return ((Collection<?>) rows).isEmpty();
    }
    return !rows.iterator().hasNext();
  }

  /**
   * Defines the set of fields to extract for the join key, as well as other per-input join options.
   */
  @AutoValue
  public abstract static class By implements Serializable {
    abstract FieldAccessDescriptor getFieldAccessDescriptor();

    abstract boolean getOptionalParticipation();

    abstract boolean getSideInput();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setFieldAccessDescriptor(FieldAccessDescriptor fieldAccessDescriptor);

      abstract Builder setOptionalParticipation(boolean optionalParticipation);

      abstract Builder setSideInput(boolean sideInput);

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
          .setSideInput(false)
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

    public By withSideInput() {
      return toBuilder().setSideInput(true).build();
    }
  }

  static class JoinArguments implements Serializable {
    private final @Nullable By allInputsJoinArgs;
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

    private @Nullable FieldAccessDescriptor getFieldAccessDescriptor(String tag) {
      return (allInputsJoinArgs != null)
          ? allInputsJoinArgs.getFieldAccessDescriptor()
          : joinArgsMap.get(tag).getFieldAccessDescriptor();
    }

    private boolean getOptionalParticipation(String tag) {
      return (allInputsJoinArgs != null)
          ? allInputsJoinArgs.getOptionalParticipation()
          : joinArgsMap.get(tag).getOptionalParticipation();
    }

    private boolean getSideInputSource(String tag) {
      return (allInputsJoinArgs != null)
          ? allInputsJoinArgs.getSideInput()
          : joinArgsMap.get(tag).getSideInput();
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
  static class JoinInformation implements Serializable {
    private final transient KeyedPCollectionTuple<Row> keyedPCollectionTuple;
    private final Map<String, PCollectionView<Map<Row, Iterable<Row>>>> sideInputs;
    private final Schema keySchema;
    private final Map<String, Schema> componentSchemas;
    private final List<String> sortedTags;
    private final Map<Integer, String> tagToKeyedTag;
    private final Map<Integer, TupleTag<Row>> indexToTupleTag;
    private final transient @Nullable PCollection<?> singleMainInput;
    private final @Nullable FieldAccessDescriptor singleMainKeyDescriptor;
    private final int singleMainIndex;

    private JoinInformation(
        KeyedPCollectionTuple<Row> keyedPCollectionTuple,
        Map<String, PCollectionView<Map<Row, Iterable<Row>>>> sideInputs,
        Schema keySchema,
        Map<String, Schema> componentSchemas,
        List<String> sortedTags,
        Map<Integer, String> tagToKeyedTag,
        Map<Integer, TupleTag<Row>> indexToTupleTag,
        @Nullable PCollection<?> singleMainInput,
        @Nullable FieldAccessDescriptor singleMainKeyDescriptor,
        int singleMainIndex) {
      this.keyedPCollectionTuple = keyedPCollectionTuple;
      this.sideInputs = sideInputs;
      this.keySchema = keySchema;
      this.componentSchemas = componentSchemas;
      this.sortedTags = sortedTags;
      this.tagToKeyedTag = tagToKeyedTag;
      this.indexToTupleTag = indexToTupleTag;
      this.singleMainInput = singleMainInput;
      this.singleMainKeyDescriptor = singleMainKeyDescriptor;
      this.singleMainIndex = singleMainIndex;
    }

    private static JoinInformation from(
        PCollectionTuple input,
        Function<String, FieldAccessDescriptor> getFieldAccessDescriptor,
        Function<String, Boolean> getIsSideInput,
        boolean deferSingleMainKeyExtraction) {
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

      Map<String, PCollectionView<Map<Row, Iterable<Row>>>> sideInputs = Maps.newHashMap();
      Map<Integer, String> tagToKeyedTag = Maps.newHashMap();
      Map<Integer, TupleTag<Row>> indexToTupleTag = Maps.newHashMap();
      Schema keySchema = null;
      @Nullable Schema mainKeySchema = null;
      int mainCount = 0;
      for (Map.Entry<TupleTag<?>, PCollection<?>> entry : input.getAll().entrySet()) {
        String tag = entry.getKey().getId();
        PCollection<?> pc = entry.getValue();
        Schema schema = pc.getSchema();
        componentSchemas.put(tag, schema);
        FieldAccessDescriptor fieldAccessDescriptor = getFieldAccessDescriptor.apply(tag);
        if (fieldAccessDescriptor == null) {
          throw new IllegalStateException("No fields were set for input " + tag);
        }
        if (!getIsSideInput.apply(tag)) {
          mainCount++;
        }
        // Resolve the key schema, keeping the fields in the order specified by the user.
        // Otherwise, if different field names are specified for different PCollections, they
        // might not match up.
        // The key schema contains the field names from the first PCollection specified.
        FieldAccessDescriptor resolved = fieldAccessDescriptor.resolve(schema);
        Schema currentKeySchema = SelectHelpers.getOutputSchema(schema, resolved);
        if (!getIsSideInput.apply(tag)) {
          mainKeySchema = currentKeySchema;
        }
        if (keySchema == null) {
          keySchema = currentKeySchema;
        } else {
          keySchema = SchemaUtils.mergeWideningNullable(keySchema, currentKeySchema);
        }
      }
      // Only fuse single-main broadcast key extraction when nested composite key schemas match
      // keySchema exactly (otherwise extractKey + SchemaCoder.of(keySchema) re-stamps nested
      // schemas so Row.equals matches the broadcast multimap keys).
      boolean fuseSingleMain =
          deferSingleMainKeyExtraction
              && mainCount == 1
              && keySchema != null
              && mainKeySchema != null
              && (!hasCompositeField(keySchema) || mainKeySchema.equals(keySchema));
      PCollection<?> singleMainPc = null;
      FieldAccessDescriptor singleMainDescriptor = null;
      int singleMainIdx = -1;
      // Second loop so we can widen the keySchema with every input before using it
      for (Map.Entry<TupleTag<?>, PCollection<?>> entry : input.getAll().entrySet()) {
        String tag = entry.getKey().getId();
        int tagIndex = sortedTags.indexOf(tag);
        PCollection<?> pc = entry.getValue();
        Schema schema = pc.getSchema();
        FieldAccessDescriptor fieldAccessDescriptor = getFieldAccessDescriptor.apply(tag);
        FieldAccessDescriptor resolved = fieldAccessDescriptor.resolve(schema);

        // Create a new tag for the output.
        TupleTag<?> randomTag = new TupleTag<>();
        String keyedTag = tag + "_" + randomTag;
        tagToKeyedTag.put(tagIndex, keyedTag);
        if (getIsSideInput.apply(tag)) {
          PCollection<KV<Row, Row>> keyedPCollection =
              extractKey(pc, schema, keySchema, resolved, tag);
          sideInputs.put(
              keyedTag, keyedPCollection.apply("computeSideInputView" + tag, View.asMultimap()));
        } else if (fuseSingleMain) {
          singleMainPc = pc;
          singleMainDescriptor = resolved;
          singleMainIdx = tagIndex;
        } else {
          TupleTag<Row> rowTag = new TupleTag<>(keyedTag);
          indexToTupleTag.put(tagIndex, rowTag);
          PCollection<KV<Row, Row>> keyedPCollection =
              extractKey(pc, schema, keySchema, resolved, tag);
          keyedPCollectionTuple = keyedPCollectionTuple.and(rowTag, keyedPCollection);
        }
      }
      return new JoinInformation(
          keyedPCollectionTuple,
          sideInputs,
          keySchema,
          componentSchemas,
          sortedTags,
          tagToKeyedTag,
          indexToTupleTag,
          singleMainPc,
          singleMainDescriptor,
          singleMainIdx);
    }

    private static <T> PCollection<KV<Row, Row>> extractKey(
        PCollection<T> pCollection,
        Schema schema,
        Schema keySchema,
        FieldAccessDescriptor keyFields,
        String tag) {
      @Nullable SerializableFunction<T, Row> toRowFn =
          SchemaUtils.isIdentityFunction(pCollection.getToRowFunction())
              ? null
              : pCollection.getToRowFunction();
      RowSelectorContainer selectorContainer =
          new RowSelectorContainer(schema, keyFields, keySchema, true);
      return pCollection
          .apply(
              "extractKey" + tag,
              ParDo.of(
                  new DoFn<T, KV<Row, Row>>() {
                    private transient @Nullable RowSelector rowSelector;

                    @Setup
                    public void setup() {
                      this.rowSelector = selectorContainer.getOrCreateSelector();
                    }

                    @ProcessElement
                    public void process(@Element T element, OutputReceiver<KV<Row, Row>> o) {
                      RowSelector selector = this.rowSelector;
                      if (selector == null) {
                        selector = selectorContainer.getOrCreateSelector();
                        this.rowSelector = selector;
                      }
                      Row row = toRowFn != null ? toRowFn.apply(element) : (Row) element;
                      o.output(KV.of(selector.select(row), row));
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

  private static boolean hasCompositeField(Schema schema) {
    for (int i = 0; i < schema.getFieldCount(); i++) {
      Schema.TypeName tn = schema.getField(i).getType().getTypeName();
      if (tn == Schema.TypeName.ROW
          || tn == Schema.TypeName.ARRAY
          || tn == Schema.TypeName.ITERABLE
          || tn == Schema.TypeName.MAP) {
        return true;
      }
    }
    return false;
  }

  /** Schema helpers for CoGroup output construction. */
  @AutoValue
  public abstract static class Result {
    abstract Row getKey();

    abstract List<Iterable<Row>> getIterables();

    abstract List<String> getTags();

    abstract JoinArguments getJoinArguments();

    abstract Schema getOutputSchema();

    static Schema getUnexandedOutputSchema(String keyFieldName, JoinInformation joinInformation) {
      Schema.Builder schemaBuilder =
          Schema.builder().addRowField(keyFieldName, joinInformation.keySchema);
      for (Map.Entry<String, Schema> entry : joinInformation.componentSchemas.entrySet()) {
        schemaBuilder.addIterableField(entry.getKey(), FieldType.row(entry.getValue()));
      }
      return schemaBuilder.build();
    }

    static void verifyExpandedArgs(JoinInformation joinInformation, JoinArguments joinArgs) {
      boolean hasSideInput = false;
      boolean allMainInputsOptional = true;
      for (int i = 0; i < joinInformation.sortedTags.size(); ++i) {
        String tupleTag = joinInformation.tagToKeyedTag.get(i);
        if (joinInformation.sideInputs.get(tupleTag) != null) {
          hasSideInput = true;
        } else if (!joinArgs.getOptionalParticipation(joinInformation.sortedTags.get(i))) {
          allMainInputsOptional = false;
        }
      }
      Preconditions.checkArgument(
          !hasSideInput || !allMainInputsOptional,
          "Cannot perform join when all main inputs are optional and there is a side input. "
              + " consider removing the side input.");
    }

    static Schema getExpandedOutputSchema(JoinInformation joinInformation, JoinArguments joinArgs) {
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
  }

  private static void emitCrossProduct(
      int index,
      int fixedIndex,
      @Nullable Object[] buffer,
      Iterable<Row>[] iterables,
      Schema outputSchema,
      OutputReceiver<Row> o) {
    boolean atBottom = index == iterables.length - 1;
    if (index == fixedIndex) {
      if (atBottom) {
        o.output(Row.attachValues(outputSchema, buffer.clone()));
      } else {
        emitCrossProduct(index + 1, fixedIndex, buffer, iterables, outputSchema, o);
      }
      return;
    }
    for (Row row : iterables[index]) {
      buffer[index] = row;
      if (atBottom) {
        o.output(Row.attachValues(outputSchema, buffer.clone()));
      } else {
        emitCrossProduct(index + 1, fixedIndex, buffer, iterables, outputSchema, o);
      }
    }
  }

  static class ConvertCoGbkResult extends DoFn<KV<Row, CoGbkResult>, Row> {
    private final int numInputs;
    private final @Nullable TupleTag<Row>[] mainTags;
    private final @Nullable PCollectionView<Map<Row, Iterable<Row>>>[] sideViews;
    private final boolean[] isOptional;
    private final Schema outputSchema;
    private final boolean unexpanded;

    enum ConvertType {
      UNEXPANDED,
      EXPANDED
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public ConvertCoGbkResult(
        JoinInformation joinInformation,
        JoinArguments joinArgs,
        ConvertType convertType,
        Schema outputSchema) {
      this.numInputs = joinInformation.sortedTags.size();
      this.mainTags = new TupleTag[numInputs];
      this.sideViews = new PCollectionView[numInputs];
      this.isOptional = new boolean[numInputs];
      for (int i = 0; i < numInputs; i++) {
        String tag = joinInformation.sortedTags.get(i);
        String keyedTag = joinInformation.tagToKeyedTag.get(i);
        PCollectionView<Map<Row, Iterable<Row>>> sideView =
            joinInformation.sideInputs.get(keyedTag);
        if (sideView != null) {
          this.sideViews[i] = sideView;
        } else {
          TupleTag<Row> existingTag = joinInformation.indexToTupleTag.get(i);
          this.mainTags[i] = existingTag != null ? existingTag : new TupleTag<>(keyedTag);
        }
        this.isOptional[i] = joinArgs.getOptionalParticipation(tag);
      }
      this.outputSchema = outputSchema;
      this.unexpanded = convertType == ConvertType.UNEXPANDED;
    }

    @ProcessElement
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void process(
        @Element KV<Row, CoGbkResult> element, ProcessContext c, OutputReceiver<Row> o) {
      Row key = element.getKey();
      CoGbkResult coGbk = element.getValue();
      if (unexpanded) {
        Object[] fields = new Object[numInputs + 1];
        fields[0] = key;
        for (int i = 0; i < numInputs; i++) {
          Iterable<Row> rows;
          PCollectionView<Map<Row, Iterable<Row>>> sideView = sideViews[i];
          if (sideView != null) {
            rows = c.sideInput(sideView).get(key);
            if (rows == null) {
              rows = Collections.emptyList();
            }
          } else {
            rows = coGbk.getAll(mainTags[i]);
          }
          fields[i + 1] = rows;
        }
        o.output(Row.attachValues(outputSchema, fields));
        return;
      }

      if (numInputs == 2) {
        Iterable<Row> r0 =
            sideViews[0] != null ? c.sideInput(sideViews[0]).get(key) : coGbk.getAll(mainTags[0]);
        if (r0 == null || isEmpty(r0)) {
          if (!isOptional[0]) {
            return;
          }
          r0 = NULL_LIST;
        }
        Iterable<Row> r1 =
            sideViews[1] != null ? c.sideInput(sideViews[1]).get(key) : coGbk.getAll(mainTags[1]);
        if (r1 == null || isEmpty(r1)) {
          if (!isOptional[1]) {
            return;
          }
          r1 = NULL_LIST;
        }
        for (Row left : r0) {
          for (Row right : r1) {
            o.output(Row.attachValues(outputSchema, left, right));
          }
        }
        return;
      }

      if (numInputs == 0) {
        return;
      }
      Iterable<Row>[] iterables = new Iterable[numInputs];
      for (int i = 0; i < numInputs; i++) {
        PCollectionView<Map<Row, Iterable<Row>>> sideView = sideViews[i];
        Iterable<Row> rows =
            sideView != null ? c.sideInput(sideView).get(key) : coGbk.getAll(mainTags[i]);
        if (rows == null || isEmpty(rows)) {
          if (!isOptional[i]) {
            // Short-circuit: empty required side means the entire cross product is empty.
            return;
          }
          rows = NULL_LIST;
        }
        iterables[i] = rows;
      }
      @Nullable Object[] buffer = new Object[numInputs];
      emitCrossProduct(0, -1, buffer, iterables, outputSchema, o);
    }
  }

  /**
   * Fused single-main-input broadcast join {@link DoFn} that extracts the join key directly from
   * each input element without an extra {@code extractKey} {@code ParDo} and {@code KV} allocation.
   */
  static class FusedExpandBroadcastJoin<T> extends DoFn<T, Row> {
    private final int numInputs;
    private final int mainIndex;
    private final RowSelectorContainer keySelectorContainer;
    private final @Nullable SerializableFunction<T, Row> toRowFunction;
    private final @Nullable PCollectionView<Map<Row, Iterable<Row>>>[] sideViews;
    private final boolean[] isOptional;
    private final Schema outputSchema;
    private transient @Nullable RowSelector keySelector;

    @SuppressWarnings({"unchecked", "rawtypes"})
    FusedExpandBroadcastJoin(
        JoinInformation joinInformation,
        JoinArguments joinArgs,
        Schema mainSchema,
        FieldAccessDescriptor keyFields,
        @Nullable SerializableFunction<T, Row> toRowFunction,
        Schema outputSchema) {
      this.numInputs = joinInformation.sortedTags.size();
      this.mainIndex = joinInformation.singleMainIndex;
      this.keySelectorContainer =
          new RowSelectorContainer(mainSchema, keyFields, joinInformation.keySchema, true);
      this.toRowFunction = toRowFunction;
      this.sideViews = new PCollectionView[numInputs];
      this.isOptional = new boolean[numInputs];
      for (int i = 0; i < numInputs; i++) {
        String tag = joinInformation.sortedTags.get(i);
        this.isOptional[i] = joinArgs.getOptionalParticipation(tag);
        if (i != mainIndex) {
          String keyedTag = joinInformation.tagToKeyedTag.get(i);
          this.sideViews[i] = joinInformation.sideInputs.get(keyedTag);
        }
      }
      this.outputSchema = outputSchema;
    }

    @Setup
    public void setup() {
      this.keySelector = keySelectorContainer.getOrCreateSelector();
    }

    @ProcessElement
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void process(@Element T element, ProcessContext c, OutputReceiver<Row> o) {
      Row mainRow = toRowFunction != null ? toRowFunction.apply(element) : (Row) element;
      if (numInputs == 1) {
        o.output(Row.attachValues(outputSchema, mainRow));
        return;
      }
      RowSelector selector = this.keySelector;
      if (selector == null) {
        selector = keySelectorContainer.getOrCreateSelector();
        this.keySelector = selector;
      }
      Row key = selector.select(mainRow);

      if (numInputs == 2) {
        int sideIdx = 1 - mainIndex;
        Iterable<Row> matches = c.sideInput(sideViews[sideIdx]).get(key);
        if (matches == null || isEmpty(matches)) {
          if (isOptional[sideIdx]) {
            if (mainIndex == 0) {
              o.output(Row.attachValues(outputSchema, mainRow, null));
            } else {
              o.output(Row.attachValues(outputSchema, null, mainRow));
            }
          }
          return;
        }
        if (mainIndex == 0) {
          for (Row sideRow : matches) {
            o.output(Row.attachValues(outputSchema, mainRow, sideRow));
          }
        } else {
          for (Row sideRow : matches) {
            o.output(Row.attachValues(outputSchema, sideRow, mainRow));
          }
        }
        return;
      }

      Iterable<Row>[] iterables = new Iterable[numInputs];
      for (int i = 0; i < numInputs; i++) {
        if (i == mainIndex) {
          continue;
        }
        Iterable<Row> matches = c.sideInput(sideViews[i]).get(key);
        if (matches == null || isEmpty(matches)) {
          if (!isOptional[i]) {
            return;
          }
          matches = NULL_LIST;
        }
        iterables[i] = matches;
      }
      @Nullable Object[] buffer = new Object[numInputs];
      buffer[mainIndex] = mainRow;
      emitCrossProduct(0, mainIndex, buffer, iterables, outputSchema, o);
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
          JoinInformation.from(
              input, joinArgs::getFieldAccessDescriptor, joinArgs::getSideInputSource, false);

      Collection<PCollectionView<Map<Row, Iterable<Row>>>> views =
          joinInformation.sideInputs.values();
      Schema outputSchema = Result.getUnexandedOutputSchema(keyFieldName, joinInformation);
      return joinInformation
          .keyedPCollectionTuple
          .apply("CoGroupByKey", CoGroupByKey.create())
          .apply(
              ParDo.of(
                      new ConvertCoGbkResult(
                          joinInformation, joinArgs, ConvertType.UNEXPANDED, outputSchema))
                  .withSideInputs(views))
          .setRowSchema(outputSchema);
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

    @Override
    public PCollection<Row> expand(PCollectionTuple input) {
      verify(input, joinArgs);

      JoinInformation joinInformation =
          JoinInformation.from(
              input, joinArgs::getFieldAccessDescriptor, joinArgs::getSideInputSource, true);

      Result.verifyExpandedArgs(joinInformation, joinArgs);
      Schema outputSchema = Result.getExpandedOutputSchema(joinInformation, joinArgs);
      Collection<PCollectionView<Map<Row, Iterable<Row>>>> views =
          joinInformation.sideInputs.values();

      PCollection<Row> expanded;
      if (joinInformation.singleMainInput != null) {
        expanded = applyFusedBroadcastJoin(joinInformation, joinArgs, outputSchema, views);
      } else {
        expanded =
            joinInformation
                .keyedPCollectionTuple
                .apply("CoGroupByKey", CoGroupByKey.create())
                .apply(
                    ParDo.of(
                            new ConvertCoGbkResult(
                                joinInformation, joinArgs, ConvertType.EXPANDED, outputSchema))
                        .withSideInputs(views));
      }
      return expanded.setRowSchema(outputSchema);
    }

    @SuppressWarnings("unchecked")
    private static <T> PCollection<Row> applyFusedBroadcastJoin(
        JoinInformation joinInformation,
        JoinArguments joinArgs,
        Schema outputSchema,
        Collection<PCollectionView<Map<Row, Iterable<Row>>>> views) {
      PCollection<T> mainPc = (PCollection<T>) joinInformation.singleMainInput;
      Schema mainSchema = mainPc.getSchema();
      @Nullable SerializableFunction<T, Row> toRowFn =
          SchemaUtils.isIdentityFunction(mainPc.getToRowFunction())
              ? null
              : mainPc.getToRowFunction();
      return mainPc.apply(
          ParDo.of(
                  new FusedExpandBroadcastJoin<>(
                      joinInformation,
                      joinArgs,
                      mainSchema,
                      joinInformation.singleMainKeyDescriptor,
                      toRowFn,
                      outputSchema))
              .withSideInputs(views));
    }
  }
}
