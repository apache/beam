/*
 * Copyright (C) 2015 Google Inc.
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

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult.CoGbkResultCoder;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple.TaggedKeyedPCollection;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link PTransform} that performs a {@link CoGroupByKey} on a tuple
 * of tables.  A {@link CoGroupByKey} groups results from all
 * tables by like keys into {@link CoGbkResult}s,
 * from which the results for any specific table can be accessed by the
 * {@link com.google.cloud.dataflow.sdk.values.TupleTag}
 * supplied with the initial table.
 *
 * <p>Example of performing a {@link CoGroupByKey} followed by a
 * {@link ParDo} that consumes
 * the results:
 * <pre> {@code
 * PCollection<KV<K, V1>> pt1 = ...;
 * PCollection<KV<K, V2>> pt2 = ...;
 *
 * final TupleTag<V1> t1 = new TupleTag<>();
 * final TupleTag<V2> t2 = new TupleTag<>();
 * PCollection<KV<K, CoGbkResult>> coGbkResultCollection =
 *   KeyedPCollectionTuple.of(t1, pt1)
 *                        .and(t2, pt2)
 *                        .apply(CoGroupByKey.<K>create());
 *
 * PCollection<T> finalResultCollection =
 *   coGbkResultCollection.apply(ParDo.of(
 *     new DoFn<KV<K, CoGbkResult>, T>() {
 *       @Override
 *       public void processElement(ProcessContext c) {
 *         KV<K, CoGbkResult> e = c.element();
 *         Iterable<V1> pt1Vals = e.getValue().getAll(t1);
 *         V2 pt2Val = e.getValue().getOnly(t2);
 *          ... Do Something ....
 *         c.output(...some T...);
 *       }
 *     }));
 * } </pre>
 *
 * @param <K> the type of the keys in the input and output
 * {@code PCollection}s
 */
public class CoGroupByKey<K> extends
    PTransform<KeyedPCollectionTuple<K>,
               PCollection<KV<K, CoGbkResult>>> {
  /**
   * Returns a {@code CoGroupByKey<K>} {@code PTransform}.
   *
   * @param <K> the type of the keys in the input and output
   * {@code PCollection}s
   */
  public static <K> CoGroupByKey<K> create() {
    return new CoGroupByKey<>();
  }

  private CoGroupByKey() { }

  @Override
  public PCollection<KV<K, CoGbkResult>> apply(
      KeyedPCollectionTuple<K> input) {
    if (input.isEmpty()) {
      throw new IllegalArgumentException(
          "must have at least one input to a KeyedPCollections");
    }

    // First build the union coder.
    // TODO: Look at better integration of union types with the
    // schema specified in the input.
    List<Coder<?>> codersList = new ArrayList<>();
    for (TaggedKeyedPCollection<K, ?> entry : input.getKeyedCollections()) {
      codersList.add(getValueCoder(entry.pCollection));
    }
    UnionCoder unionCoder = UnionCoder.of(codersList);
    Coder<K> keyCoder = input.getKeyCoder();
    KvCoder<K, RawUnionValue> kVCoder =
        KvCoder.of(keyCoder, unionCoder);

    PCollectionList<KV<K, RawUnionValue>> unionTables =
        PCollectionList.empty(input.getPipeline());

    // TODO: Use the schema to order the indices rather than depending
    // on the fact that the schema ordering is identical to the ordering from
    // input.getJoinCollections().
    int index = -1;
    for (TaggedKeyedPCollection<K, ?> entry : input.getKeyedCollections()) {
      index++;
      PCollection<KV<K, RawUnionValue>> unionTable =
          makeUnionTable(index, entry.pCollection, kVCoder);
      unionTables = unionTables.and(unionTable);
    }

    PCollection<KV<K, RawUnionValue>> flattenedTable =
        unionTables.apply(Flatten.<KV<K, RawUnionValue>>pCollections());

    PCollection<KV<K, Iterable<RawUnionValue>>> groupedTable =
        flattenedTable.apply(GroupByKey.<K, RawUnionValue>create());

    CoGbkResultSchema tupleTags = input.getCoGbkResultSchema();
    PCollection<KV<K, CoGbkResult>> result = groupedTable.apply(
        ParDo.of(new ConstructCoGbkResultFn<K>(tupleTags))
          .named("ConstructCoGbkResultFn"));
    result.setCoder(KvCoder.of(keyCoder,
        CoGbkResultCoder.of(tupleTags, unionCoder)));

    return result;
  }

  //////////////////////////////////////////////////////////////////////////////

  /**
   * Returns the value coder for the given PCollection.  Assumes that the value
   * coder is an instance of {@code KvCoder<K, V>}.
   */
  private <V> Coder<V> getValueCoder(PCollection<KV<K, V>> pCollection) {
    // Assumes that the PCollection uses a KvCoder.
    Coder<?> entryCoder = pCollection.getCoder();
    if (!(entryCoder instanceof KvCoder<?, ?>)) {
      throw new IllegalArgumentException("PCollection does not use a KvCoder");
    }
    @SuppressWarnings("unchecked")
    KvCoder<K, V> coder = (KvCoder<K, V>) entryCoder;
    return coder.getValueCoder();
  }

  /**
   * Returns a UnionTable for the given input PCollection, using the given
   * union index and the given unionTableEncoder.
   */
  private <V> PCollection<KV<K, RawUnionValue>> makeUnionTable(
      final int index,
      PCollection<KV<K, V>> pCollection,
      KvCoder<K, RawUnionValue> unionTableEncoder) {

    return pCollection.apply(ParDo.of(
        new ConstructUnionTableFn<K, V>(index)).named("MakeUnionTable" + index))
                                               .setCoder(unionTableEncoder);
  }

  /**
   * A DoFn to construct a UnionTable (i.e., a
   * {@code PCollection<KV<K, RawUnionValue>>} from a
   * {@code PCollection<KV<K, V>>}.
   */
  private static class ConstructUnionTableFn<K, V> extends
      DoFn<KV<K, V>, KV<K, RawUnionValue>> {

    private final int index;

    public ConstructUnionTableFn(int index) {
      this.index = index;
    }

    @Override
    public void processElement(ProcessContext c) {
      KV<K, ?> e = c.element();
      c.output(KV.of(e.getKey(), new RawUnionValue(index, e.getValue())));
    }
  }

  /**
   * A DoFn to construct a CoGbkResult from an input grouped union
   * table.
    */
  private static class ConstructCoGbkResultFn<K>
    extends DoFn<KV<K, Iterable<RawUnionValue>>,
                 KV<K, CoGbkResult>> {

    private final CoGbkResultSchema schema;

    public ConstructCoGbkResultFn(CoGbkResultSchema schema) {
      this.schema = schema;
    }

    @Override
    public void processElement(ProcessContext c) {
      KV<K, Iterable<RawUnionValue>> e = c.element();
      c.output(KV.of(e.getKey(), new CoGbkResult(schema, e.getValue())));
    }
  }
}
