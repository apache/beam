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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;

/**
 * A transform that performs equijoins across multiple schema {@link PCollection}s.
 *
 * <p>This transform has similarites to {@link CoGroupByKey}, however works on PCollections that
 * have schemas. This allows users of the transform to simply specify schema fields to join on. The
 * output type of the transform is a {@literal KV<Row, Row>} where the value contains one field for
 * every input PCollection and the key represents the fields that were joined on. By default the
 * cross product is not expanded, so all fields in the output row are array fields.
 *
 * <p>For example, the following demonstrates joining three PCollections on the "user" and "country"
 * fields.
 *
 * <pre>{@code
 * TupleTag<Input1Type> input1Tag = new TupleTag<>("input1");
 * TupleTag<Input2Type> input2Tag = new TupleTag<>("input2");
 * TupleTag<Input3Type> input3Tag = new TupleTag<>("input3");
 * PCollection<KV<Row, Row>> joined = PCollectionTuple
 *     .of(input1Tag, input1)
 *     .and(input2Tag, input2)
 *     .and(input3Tag, input3)
 *   .apply(CoGroup.byFieldNames("user", "country"));
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
 * <pre>{@code
 * {@literal @}DefaultSchema(JavaFieldSchema.class)
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
 * The value schema is convertible to the following POJO:
 *
 * <pre>{@code
 * {@literal @}DefaultSchema(JavaFieldSchema.class)
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
 * <pre>{@code
 * PCollection<KV<Row, Row>> joined = PCollectionTuple
 *     .of(input1Tag, input1)
 *     .and(input2Tag, input2)
 *   .apply(CoGroup
 *     .byFieldNames(input1Tag, "referringUser"))
 *     .byFieldNames(input2Tag, "user"));
 * }</pre>
 */
public class CoGroup {
  /**
   * Join by the following field names.
   *
   * <p>The same field names are used in all input PCollections.
   */
  public static Inner byFieldNames(String... fieldNames) {
    return byFieldAccessDescriptor(FieldAccessDescriptor.withFieldNames(fieldNames));
  }

  /**
   * Join by the following field ids.
   *
   * <p>The same field ids are used in all input PCollections.
   */
  public static Inner byFieldIds(Integer... fieldIds) {
    return byFieldAccessDescriptor(FieldAccessDescriptor.withFieldIds(fieldIds));
  }

  /**
   * Join by the following {@link FieldAccessDescriptor}.
   *
   * <p>The same access descriptor is used in all input PCollections.
   */
  public static Inner byFieldAccessDescriptor(FieldAccessDescriptor fieldAccessDescriptor) {
    return new Inner(fieldAccessDescriptor);
  }

  /**
   * Select the following field names for the specified PCollection.
   *
   * <p>Each PCollection in the input must have fields specified for the join key.
   */
  public static Inner byFieldNames(TupleTag<?> tag, String... fieldNames) {
    return byFieldAccessDescriptor(tag, FieldAccessDescriptor.withFieldNames(fieldNames));
  }

  /**
   * Select the following field ids for the specified PCollection.
   *
   * <p>Each PCollection in the input must have fields specified for the join key.
   */
  public static Inner byFieldIds(TupleTag<?> tag, Integer... fieldIds) {
    return byFieldAccessDescriptor(tag, FieldAccessDescriptor.withFieldIds(fieldIds));
  }

  /**
   * Select the following fields for the specified PCollection using {@link FieldAccessDescriptor}.
   *
   * <p>Each PCollection in the input must have fields specified for the join key.
   */
  public static Inner byFieldAccessDescriptor(
      TupleTag<?> tag, FieldAccessDescriptor fieldAccessDescriptor) {
    return new Inner().byFieldAccessDescriptor(tag, fieldAccessDescriptor);
  }

  /** The implementing PTransform. */
  public static class Inner extends PTransform<PCollectionTuple, PCollection<KV<Row, Row>>> {
    @Nullable private final FieldAccessDescriptor allInputsFieldAccessDescriptor;
    private final Map<TupleTag<?>, FieldAccessDescriptor> fieldAccessDescriptorMap;

    private Inner() {
      this(Collections.emptyMap());
    }

    private Inner(Map<TupleTag<?>, FieldAccessDescriptor> fieldAccessDescriptorMap) {
      this.allInputsFieldAccessDescriptor = null;
      this.fieldAccessDescriptorMap = fieldAccessDescriptorMap;
    }

    private Inner(FieldAccessDescriptor allInputsFieldAccessDescriptor) {
      this.allInputsFieldAccessDescriptor = allInputsFieldAccessDescriptor;
      this.fieldAccessDescriptorMap = Collections.emptyMap();
    }

    /**
     * Join by the following field names.
     *
     * <p>The same field names are used in all input PCollections.
     */
    public Inner byFieldNames(TupleTag<?> tag, String... fieldNames) {
      return byFieldAccessDescriptor(tag, FieldAccessDescriptor.withFieldNames(fieldNames));
    }

    /**
     * Select the following field ids for the specified PCollection.
     *
     * <p>Each PCollection in the input must have fields specified for the join key.
     */
    public Inner byFieldIds(TupleTag<?> tag, Integer... fieldIds) {
      return byFieldAccessDescriptor(tag, FieldAccessDescriptor.withFieldIds(fieldIds));
    }

    /**
     * Select the following fields for the specified PCollection using {@link
     * FieldAccessDescriptor}.
     *
     * <p>Each PCollection in the input must have fields specified for the join key.
     */
    public Inner byFieldAccessDescriptor(
        TupleTag<?> tag, FieldAccessDescriptor fieldAccessDescriptor) {
      if (allInputsFieldAccessDescriptor != null) {
        throw new IllegalStateException("Cannot set both a global and per-tag fields.");
      }
      return new Inner(
          new ImmutableMap.Builder<TupleTag<?>, FieldAccessDescriptor>()
              .putAll(fieldAccessDescriptorMap)
              .put(tag, fieldAccessDescriptor)
              .build());
    }

    @Nullable
    private FieldAccessDescriptor getFieldAccessDescriptor(TupleTag<?> tag) {
      return (allInputsFieldAccessDescriptor != null)
          ? allInputsFieldAccessDescriptor
          : fieldAccessDescriptorMap.get(tag);
    }

    @Override
    public PCollection<KV<Row, Row>> expand(PCollectionTuple input) {
      KeyedPCollectionTuple<Row> keyedPCollectionTuple =
          KeyedPCollectionTuple.empty(input.getPipeline());
      List<TupleTag<Row>> sortedTags =
          input
              .getAll()
              .keySet()
              .stream()
              .sorted(Comparator.comparing(TupleTag::getId))
              .map(t -> new TupleTag<Row>(t.getId() + "_ROW"))
              .collect(Collectors.toList());

      // Keep this in a TreeMap so that it's sorted. This way we get a deterministic output
      // schema.
      TreeMap<String, Schema> componentSchemas = Maps.newTreeMap();
      Map<String, SerializableFunction<Object, Row>> toRows = Maps.newHashMap();

      Schema keySchema = null;
      for (Map.Entry<TupleTag<?>, PCollection<?>> entry : input.getAll().entrySet()) {
        TupleTag<?> tag = entry.getKey();
        PCollection<?> pc = entry.getValue();
        Schema schema = pc.getSchema();
        componentSchemas.put(tag.getId(), schema);
        TupleTag<Row> rowTag = new TupleTag<>(tag.getId() + "_ROW");
        toRows.put(rowTag.getId(), (SerializableFunction<Object, Row>) pc.getToRowFunction());
        FieldAccessDescriptor fieldAccessDescriptor = getFieldAccessDescriptor(tag);
        if (fieldAccessDescriptor == null) {
          throw new IllegalStateException("No fields were set for input " + tag);
        }
        // Resolve the key schema, keeping the fields in the order specified by the user.
        // Otherwise, if different field names are specified for different PCollections, they
        // might not match up.
        // The key schema contains the field names from the first PCollection specified.
        FieldAccessDescriptor resolved =
            fieldAccessDescriptor.withOrderByFieldInsertionOrder().resolve(schema);
        Schema currentKeySchema = Select.getOutputSchema(schema, resolved);
        if (keySchema == null) {
          keySchema = currentKeySchema;
        } else {
          if (!currentKeySchema.typesEqual(keySchema)) {
            throw new IllegalStateException("All keys must have the same schema");
          }
        }

        PCollection<KV<Row, Row>> keyedPCollection =
            extractKey(pc, schema, keySchema, resolved, tag.getId());
        keyedPCollectionTuple = keyedPCollectionTuple.and(rowTag, keyedPCollection);
      }

      // Construct the output schema. It contains one field for each input PCollection, of type
      // ARRAY[ROW].
      Schema.Builder joinedSchemaBuilder = Schema.builder();
      for (Map.Entry<String, Schema> entry : componentSchemas.entrySet()) {
        joinedSchemaBuilder.addArrayField(entry.getKey(), FieldType.row(entry.getValue()));
      }
      Schema joinedSchema = joinedSchemaBuilder.build();

      return keyedPCollectionTuple
          .apply("CoGroupByKey", CoGroupByKey.create())
          .apply("ConvertToRow", ParDo.of(new ConvertToRow(sortedTags, toRows, joinedSchema)))
          .setCoder(KvCoder.of(SchemaCoder.of(keySchema), SchemaCoder.of(joinedSchema)));
    }

    private static class ConvertToRow extends DoFn<KV<Row, CoGbkResult>, KV<Row, Row>> {
      List<TupleTag<Row>> sortedTags;
      Map<String, SerializableFunction<Object, Row>> toRows = Maps.newHashMap();
      Schema joinedSchema;

      public ConvertToRow(
          List<TupleTag<Row>> sortedTags,
          Map<String, SerializableFunction<Object, Row>> toRows,
          Schema joinedSchema) {
        this.sortedTags = sortedTags;
        this.toRows = toRows;
        this.joinedSchema = joinedSchema;
      }

      @ProcessElement
      public void process(@Element KV<Row, CoGbkResult> kv, OutputReceiver<KV<Row, Row>> o) {
        Row key = kv.getKey();
        CoGbkResult result = kv.getValue();
        List<Object> fields = Lists.newArrayListWithExpectedSize(sortedTags.size());
        for (TupleTag<?> tag : sortedTags) {
          // TODO: This forces the entire join to materialize in memory. We should create a
          // lazy Row interface on top of the iterable returned by CoGbkResult. This will
          // allow the data to be streamed in.
          SerializableFunction<Object, Row> toRow = toRows.get(tag.getId());
          List<Row> joined = Lists.newArrayList();
          for (Object item : result.getAll(tag)) {
            joined.add(toRow.apply(item));
          }
          fields.add(joined);
        }
        o.output(KV.of(key, Row.withSchema(joinedSchema).addValues(fields).build()));
      }
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
                      o.output(KV.of(Select.selectRow(row, keyFields, schema, keySchema), row));
                    }
                  }))
          .setCoder(KvCoder.of(SchemaCoder.of(keySchema), SchemaCoder.of(schema)));
    }
  }
}
