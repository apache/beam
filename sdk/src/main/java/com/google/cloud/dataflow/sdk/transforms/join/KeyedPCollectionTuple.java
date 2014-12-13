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

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.POutput;
import com.google.cloud.dataflow.sdk.values.PValue;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.dataflow.sdk.values.TupleTagList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Represents an immutable tuple of keyed PCollections (i.e. PCollections of
 * {@code KV<K, ?>}), with key type K.
 *
 * @param <K> the type of key shared by all constituent PCollections
 */
public class KeyedPCollectionTuple<K> implements PInput {
  /**
   * Returns an empty {@code KeyedPCollections<K>} on the given pipeline.
   */
  public static <K> KeyedPCollectionTuple<K> empty(Pipeline pipeline) {
    return new KeyedPCollectionTuple<>(pipeline);
  }

  /**
   * Returns a new {@code KeyedPCollections<K>} with the given tag and initial
   * PCollection.
   */
  public static <K, VI> KeyedPCollectionTuple<K> of(
      TupleTag<VI> tag,
      PCollection<KV<K, VI>> pc) {
    return new KeyedPCollectionTuple<K>(pc.getPipeline()).and(tag, pc);
  }

  /**
   * Returns a new {@code KeyedPCollections<K>} that is the same as this,
   * appended with the given PCollection.
   */
  public <V> KeyedPCollectionTuple<K> and(
      TupleTag< V> tag,
      PCollection<KV<K, V>> pc) {
    if (pc.getPipeline() != getPipeline()) {
      throw new IllegalArgumentException(
          "PCollections come from different Pipelines");
    }
    TaggedKeyedPCollection<K, ?> wrapper =
        new TaggedKeyedPCollection<>(tag, pc);
    Coder<K> myKeyCoder = keyCoder == null ? getKeyCoder(pc) : keyCoder;
    List<TaggedKeyedPCollection<K, ?>>
      newKeyedCollections =
        copyAddLast(
            keyedCollections,
            wrapper);
    return new KeyedPCollectionTuple<>(
        getPipeline(),
        newKeyedCollections,
        schema.getTupleTagList().and(tag),
        myKeyCoder);
  }

  public boolean isEmpty() {
    return keyedCollections.isEmpty();
  }

  /**
   * Returns a list of TaggedKeyedPCollections for the PCollections contained in
   * this {@code KeyedPCollections<K>}.
   */
  public List<TaggedKeyedPCollection<K, ?>> getKeyedCollections() {
    return keyedCollections;
  }

  /**
   * Applies the given transform to this input.
   */
  public <O extends POutput> O apply(
      PTransform<KeyedPCollectionTuple<K>, O> transform) {
    return Pipeline.applyTransform(this, transform);
  }

  /**
   * Expands the component PCollections, stripping off any tag-specific
   * information.
   */
  @Override
  public Collection<? extends PValue> expand() {
    List<PCollection<?>> retval = new ArrayList<>();
    for (TaggedKeyedPCollection<K, ?> taggedPCollection : keyedCollections) {
      retval.add(taggedPCollection.pCollection);
    }
    return retval;
  }

  /**
   * Returns the KeyCoder for all PCollections in this KeyedPCollections.
   */
  public Coder<K> getKeyCoder() {
    if (keyCoder == null) {
      throw new IllegalStateException("cannot return null keyCoder");
    }
    return keyCoder;
  }

  /**
   * Returns the CoGbkResultSchema associated with this
   * KeyedPCollections.
   */
  public CoGbkResultSchema getCoGbkResultSchema() {
    return schema;
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  @Override
  public void finishSpecifying() {
    for (TaggedKeyedPCollection<K, ?> taggedPCollection : keyedCollections) {
      taggedPCollection.pCollection.finishSpecifying();
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * A utility class to help ensure coherence of tag and input PCollection
   * types.
   */
  static class TaggedKeyedPCollection<K, V> {
    final TupleTag<V> tupleTag;
    final PCollection<KV<K, V>> pCollection;

    public TaggedKeyedPCollection(
        TupleTag<V> tupleTag,
        PCollection<KV<K, V>> pCollection) {
      this.tupleTag = tupleTag;
      this.pCollection = pCollection;
    }
  }

  /**
   * We use a List to properly track the order in which collections are added.
   */
  private final List<TaggedKeyedPCollection<K, ?>> keyedCollections;

  private final Coder<K> keyCoder;

  private final CoGbkResultSchema schema;

  private final Pipeline pipeline;

  KeyedPCollectionTuple(Pipeline pipeline) {
    this(pipeline,
         new ArrayList<TaggedKeyedPCollection<K, ?>>(),
         TupleTagList.empty(),
         null);
  }

  KeyedPCollectionTuple(
      Pipeline pipeline,
      List<TaggedKeyedPCollection<K, ?>> keyedCollections,
      TupleTagList tupleTagList,
      Coder<K> keyCoder) {
    this.pipeline = pipeline;
    this.keyedCollections = keyedCollections;
    this.schema = new CoGbkResultSchema(tupleTagList);
    this.keyCoder = keyCoder;
  }

  private static <K, V> Coder<K> getKeyCoder(PCollection<KV<K, V>> pc) {
    // Need to run coder inference on this PCollection before inspecting it.
    pc.finishSpecifying();

    // Assumes that the PCollection uses a KvCoder.
    Coder<?> entryCoder = pc.getCoder();
    if (!(entryCoder instanceof KvCoder<?, ?>)) {
      throw new IllegalArgumentException("PCollection does not use a KvCoder");
    }
    @SuppressWarnings("unchecked")
    KvCoder<K, V> coder = (KvCoder<K, V>) entryCoder;
    return coder.getKeyCoder();
  }

  private static <K> List<TaggedKeyedPCollection<K, ?>> copyAddLast(
        List<TaggedKeyedPCollection<K, ?>> keyedCollections,
        TaggedKeyedPCollection<K, ?> taggedCollection) {
    List<TaggedKeyedPCollection<K, ?>> retval =
        new ArrayList<>(keyedCollections);
    retval.add(taggedCollection);
    return retval;
  }
}
