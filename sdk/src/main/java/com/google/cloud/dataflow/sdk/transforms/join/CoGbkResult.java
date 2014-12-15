/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.transforms.join;

import static com.google.cloud.dataflow.sdk.util.Structs.addObject;

import com.google.api.client.util.Preconditions;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.ListCoder;
import com.google.cloud.dataflow.sdk.coders.MapCoder;
import com.google.cloud.dataflow.sdk.coders.StandardCoder;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.dataflow.sdk.values.TupleTagList;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A row result of a CoGroupByKey.  This is a tuple of Iterables produced for
 * a given key, and these can be accessed in different ways.
 */
public class CoGbkResult {
  // TODO: If we keep this representation for any amount of time,
  // optimize it so that the union tag does not have to be repeated in the
  // values stored under the union tag key.
  /**
   * A map of integer union tags to a list of union objects.
   * Note: the key and the embedded union tag are the same, so it is redundant
   * to store it multiple times, but for now it makes encoding easier.
   */
  private final Map<Integer, List<RawUnionValue>> valueMap;

  private final CoGbkResultSchema schema;

  /**
   * A row in the PCollection resulting from a CoGroupByKey transform.
   * Currently, this row must fit into memory.
   *
   * @param schema the set of tuple tags used to refer to input tables and
   *               result values
   * @param values the raw results from a group-by-key
   */
  @SuppressWarnings("unchecked")
  public CoGbkResult(
      CoGbkResultSchema schema,
      Iterable<RawUnionValue> values) {
    this.schema = schema;
    valueMap = new TreeMap<>();
    for (RawUnionValue value : values) {
      // Make sure the given union tag has a corresponding tuple tag in the
      // schema.
      int unionTag = value.getUnionTag();
      if (schema.size() <= unionTag) {
        throw new IllegalStateException("union tag " + unionTag +
            " has no corresponding tuple tag in the result schema");
      }
      List<RawUnionValue> taggedValueList = valueMap.get(unionTag);
      if (taggedValueList == null) {
        taggedValueList = new ArrayList<>();
        valueMap.put(unionTag, taggedValueList);
      }
      taggedValueList.add(value);
    }
  }

  public boolean isEmpty() {
    return valueMap == null || valueMap.isEmpty();
  }

  /**
   * Returns the schema used by this CoGbkResult.
   */
  public CoGbkResultSchema getSchema() {
    return schema;
  }

  @Override
  public String toString() {
    return valueMap.toString();
  }

  /**
   * Returns the values from the table represented by the given
   * {@code TupleTag<V>} as an {@code Iterable<V>} (which may be empty if there
   * are no results).
   */
  public <V> Iterable<V> getAll(TupleTag<V> tag) {
    int index = schema.getIndex(tag);
    if (index < 0) {
      throw new IllegalArgumentException("TupleTag " + tag +
          " is not in the schema");
    }
    List<RawUnionValue> unions = valueMap.get(index);
    if (unions == null) {
      return buildEmptyIterable(tag);
    }
    return new UnionValueIterable<>(unions);
  }

  /**
   * If there is a singleton value for the given tag, returns it.
   * Otherwise, throws an IllegalArgumentException.
   */
  public <V> V getOnly(TupleTag<V> tag) {
    return innerGetOnly(tag, null, false);
  }

  /**
   * If there is a singleton value for the given tag, returns it.  If there is
   * no value for the given tag, returns the defaultValue.
   * Otherwise, throws an IllegalArgumentException.
   */
  public <V> V getOnly(TupleTag<V> tag, V defaultValue) {
    return innerGetOnly(tag, defaultValue, true);
  }

  /**
   * A coder for CoGbkResults.
   */
  @SuppressWarnings("serial")
  public static class CoGbkResultCoder extends StandardCoder<CoGbkResult> {

    private final CoGbkResultSchema schema;
    private final MapCoder<Integer, List<RawUnionValue>> mapCoder;

    /**
     * Returns a CoGbkResultCoder for the given schema and unionCoder.
     */
    public static CoGbkResultCoder of(
        CoGbkResultSchema schema,
        UnionCoder unionCoder) {
      return new CoGbkResultCoder(schema, unionCoder);
    }

    @JsonCreator
    public static CoGbkResultCoder of(
        @JsonProperty(PropertyNames.COMPONENT_ENCODINGS)
        List<Coder<?>> components,
        @JsonProperty(PropertyNames.CO_GBK_RESULT_SCHEMA) CoGbkResultSchema schema) {
      Preconditions.checkArgument(components.size() == 1,
          "Expecting 1 component, got " + components.size());
      return new CoGbkResultCoder(schema, (MapCoder) components.get(0));
    }

    private CoGbkResultCoder(
        CoGbkResultSchema tupleTags,
        UnionCoder unionCoder) {
      this.schema = tupleTags;
      this.mapCoder = MapCoder.of(VarIntCoder.of(),
          ListCoder.of(unionCoder));
    }

    private CoGbkResultCoder(
        CoGbkResultSchema tupleTags,
        MapCoder mapCoder) {
      this.schema = tupleTags;
      this.mapCoder = mapCoder;
    }


    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return null;
    }

    @Override
    public List<? extends Coder<?>> getComponents() {
      return Arrays.<Coder<?>>asList(mapCoder);
    }

    @Override
    public CloudObject asCloudObject() {
      CloudObject result = super.asCloudObject();
      addObject(result, PropertyNames.CO_GBK_RESULT_SCHEMA, schema.asCloudObject());
      return result;
    }

    @Override
    public void encode(
        CoGbkResult value,
        OutputStream outStream,
        Context context) throws CoderException,
        IOException {
      if (!schema.equals(value.getSchema())) {
        throw new CoderException("input schema does not match coder schema");
      }
      mapCoder.encode(value.valueMap, outStream, context);
    }

    @Override
    public CoGbkResult decode(
        InputStream inStream,
        Context context)
        throws CoderException, IOException {
      Map<Integer, List<RawUnionValue>> map = mapCoder.decode(
          inStream, context);
      return new CoGbkResult(schema, map);
    }

    public boolean equals(Object other) {
      if (!super.equals(other)) {
        return false;
      }
      return schema.equals(((CoGbkResultCoder) other).schema);
    }

    @Override
    public boolean isDeterministic() {
      return mapCoder.isDeterministic();
    }
  }


  //////////////////////////////////////////////////////////////////////////////
  // Methods for testing purposes

  /**
   * Returns a new CoGbkResult that contains just the given tag the given data.
   */
  public static <V> CoGbkResult of(TupleTag<V> tag, List<V> data) {
    return CoGbkResult.empty().and(tag, data);
  }

  /**
   * Returns a new CoGbkResult based on this, with the given tag and given data
   * added to it.
   */
  public <V> CoGbkResult and(TupleTag<V> tag, List<V> data) {
    if (nextTestUnionId != schema.size()) {
      throw new IllegalArgumentException(
          "Attempting to call and() on a CoGbkResult apparently not created by"
          + " of().");
    }
    Map<Integer, List<RawUnionValue>> valueMap = new TreeMap<>(this.valueMap);
    valueMap.put(nextTestUnionId,
        convertValueListToUnionList(nextTestUnionId, data));
    return new CoGbkResult(
        new CoGbkResultSchema(schema.getTupleTagList().and(tag)), valueMap,
        nextTestUnionId + 1);
  }

  /**
   * Returns an empty CoGbkResult.
   */
  public static <V> CoGbkResult empty() {
    return new CoGbkResult(new CoGbkResultSchema(TupleTagList.empty()),
        new TreeMap<Integer, List<RawUnionValue>>());
  }

  //////////////////////////////////////////////////////////////////////////////

  private int nextTestUnionId = 0;

  private CoGbkResult(
      CoGbkResultSchema schema,
      Map<Integer, List<RawUnionValue>> valueMap,
      int nextTestUnionId) {
    this(schema, valueMap);
    this.nextTestUnionId = nextTestUnionId;
  }

  private CoGbkResult(
      CoGbkResultSchema schema,
      Map<Integer, List<RawUnionValue>> valueMap) {
    this.schema = schema;
    this.valueMap = valueMap;
  }

  private static <V> List<RawUnionValue> convertValueListToUnionList(
      int unionTag, List<V> data) {
    List<RawUnionValue> unionList = new ArrayList<>();
    for (V value : data) {
      unionList.add(new RawUnionValue(unionTag, value));
    }
    return unionList;
  }

  private <V> Iterable<V> buildEmptyIterable(TupleTag<V> tag) {
    return new ArrayList<>();
  }

  private <V> V innerGetOnly(
      TupleTag<V> tag,
      V defaultValue,
      boolean useDefault) {
    int index = schema.getIndex(tag);
    if (index < 0) {
      throw new IllegalArgumentException("TupleTag " + tag
          + " is not in the schema");
    }
    List<RawUnionValue> unions = valueMap.get(index);
    if (unions.isEmpty()) {
      if (useDefault) {
        return defaultValue;
      } else {
        throw new IllegalArgumentException("TupleTag " + tag
            + " corresponds to an empty result, and no default was provided");
      }
    }
    if (unions.size() != 1) {
      throw new IllegalArgumentException("TupleTag " + tag
          + " corresponds to a non-singleton result of size " + unions.size());
    }
    return (V) unions.get(0).getValue();
  }

  /**
   * Lazily converts and recasts an {@code Iterable<RawUnionValue>} into an
   * {@code Iterable<V>}, where V is the type of the raw union value's contents.
   */
  private static class UnionValueIterable<V> implements Iterable<V> {

    private final Iterable<RawUnionValue> unions;

    private UnionValueIterable(Iterable<RawUnionValue> unions) {
      this.unions = unions;
    }

    @Override
    public Iterator<V> iterator() {
      final Iterator<RawUnionValue> unionsIterator = unions.iterator();
      return new Iterator<V>() {
        @Override
        public boolean hasNext() {
          return unionsIterator.hasNext();
        }

        @Override
        public V next() {
          return (V) unionsIterator.next().getValue();
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }
  }
}
