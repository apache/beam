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
package org.apache.beam.sdk.transforms.join;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An immutable tuple of keyed {@link PCollection PCollections} with key type K. ({@link PCollection
 * PCollections} containing values of type {@code KV<K, ?>})
 *
 * @param <K> the type of key shared by all constituent PCollections
 */
public class KeyedPCollectionTuple<K> implements PInput {
  /** Returns an empty {@code KeyedPCollectionTuple<K>} on the given pipeline. */
  public static <K> KeyedPCollectionTuple<K> empty(Pipeline pipeline) {
    return new KeyedPCollectionTuple<>(pipeline);
  }

  /** Returns a new {@code KeyedPCollectionTuple<K>} with the given tag and initial PCollection. */
  public static <K, InputT> KeyedPCollectionTuple<K> of(
      TupleTag<InputT> tag, PCollection<KV<K, InputT>> pc) {
    return new KeyedPCollectionTuple<K>(pc.getPipeline()).and(tag, pc);
  }

  /**
   * A version of {@link #of(TupleTag, PCollection)} that takes in a string instead of a TupleTag.
   *
   * <p>This method is simpler for cases when a typed tuple-tag is not needed to extract a
   * PCollection, for example when using schema transforms.
   */
  public static <K, InputT> KeyedPCollectionTuple<K> of(String tag, PCollection<KV<K, InputT>> pc) {
    return of(new TupleTag<>(tag), pc);
  }

  /**
   * Returns a new {@code KeyedPCollectionTuple<K>} that is the same as this, appended with the
   * given PCollection.
   */
  public <V> KeyedPCollectionTuple<K> and(TupleTag<V> tag, PCollection<KV<K, V>> pc) {
    if (pc.getPipeline() != getPipeline()) {
      throw new IllegalArgumentException("PCollections come from different Pipelines");
    }
    TaggedKeyedPCollection<K, ?> wrapper = new TaggedKeyedPCollection<>(tag, pc);
    Coder<K> myKeyCoder = keyCoder == null ? getKeyCoder(pc) : keyCoder;
    List<TaggedKeyedPCollection<K, ?>> newKeyedCollections = copyAddLast(keyedCollections, wrapper);
    return new KeyedPCollectionTuple<>(
        getPipeline(), newKeyedCollections, schema.getTupleTagList().and(tag), myKeyCoder);
  }

  /**
   * A version of {@link #and(String, PCollection)} that takes in a string instead of a TupleTag.
   *
   * <p>This method is simpler for cases when a typed tuple-tag is not needed to extract a
   * PCollection, for example when using schema transforms.
   */
  public <V> KeyedPCollectionTuple<K> and(String tag, PCollection<KV<K, V>> pc) {
    return and(new TupleTag<>(tag), pc);
  }

  public boolean isEmpty() {
    return keyedCollections.isEmpty();
  }

  /**
   * Returns a list of {@link TaggedKeyedPCollection TaggedKeyedPCollections} for the {@link
   * PCollection PCollections} contained in this {@link KeyedPCollectionTuple}.
   */
  public List<TaggedKeyedPCollection<K, ?>> getKeyedCollections() {
    return keyedCollections;
  }

  /**
   * Like {@link #apply(String, PTransform)} but defaulting to the name provided by the {@link
   * PTransform}.
   */
  public <OutputT extends POutput> OutputT apply(
      PTransform<KeyedPCollectionTuple<K>, OutputT> transform) {
    return Pipeline.applyTransform(this, transform);
  }

  /**
   * Applies the given {@link PTransform} to this input {@code KeyedPCollectionTuple} and returns
   * its {@code OutputT}. This uses {@code name} to identify the specific application of the
   * transform. This name is used in various places, including the monitoring UI, logging, and to
   * stably identify this application node in the job graph.
   */
  public <OutputT extends POutput> OutputT apply(
      String name, PTransform<KeyedPCollectionTuple<K>, OutputT> transform) {
    return Pipeline.applyTransform(name, this, transform);
  }

  /**
   * Expands the component {@link PCollection PCollections}, stripping off any tag-specific
   * information.
   */
  @Override
  public Map<TupleTag<?>, PValue> expand() {
    ImmutableMap.Builder<TupleTag<?>, PValue> retval = ImmutableMap.builder();
    for (TaggedKeyedPCollection<K, ?> taggedPCollection : keyedCollections) {
      retval.put(taggedPCollection.tupleTag, taggedPCollection.pCollection);
    }
    return retval.build();
  }

  /**
   * Returns the key {@link Coder} for all {@link PCollection PCollections} in this {@link
   * KeyedPCollectionTuple}.
   */
  public Coder<K> getKeyCoder() {
    if (keyCoder == null) {
      throw new IllegalStateException("cannot return null keyCoder");
    }
    return keyCoder;
  }

  /** Returns the {@link CoGbkResultSchema} associated with this {@link KeyedPCollectionTuple}. */
  public CoGbkResultSchema getCoGbkResultSchema() {
    return schema;
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  private static <K, V> Coder<K> getKeyCoder(PCollection<KV<K, V>> pc) {
    // TODO: This should already have run coder inference for output, but may not have been consumed
    // as input yet (and won't be fully specified); This is fine

    // Assumes that the PCollection uses a KvCoder.
    Coder<?> entryCoder = pc.getCoder();
    if (!(entryCoder instanceof KvCoder<?, ?>)) {
      throw new IllegalArgumentException("PCollection does not use a KvCoder");
    }
    @SuppressWarnings("unchecked")
    KvCoder<K, V> coder = (KvCoder<K, V>) entryCoder;
    return coder.getKeyCoder();
  }

  /////////////////////////////////////////////////////////////////////////////

  /** A utility class to help ensure coherence of tag and input PCollection types. */
  public static class TaggedKeyedPCollection<K, V> {

    final TupleTag<V> tupleTag;
    final PCollection<KV<K, V>> pCollection;

    public TaggedKeyedPCollection(TupleTag<V> tupleTag, PCollection<KV<K, V>> pCollection) {
      this.tupleTag = tupleTag;
      this.pCollection = pCollection;
    }

    /** Returns the underlying PCollection of this TaggedKeyedPCollection. */
    public PCollection<KV<K, V>> getCollection() {
      return pCollection;
    }

    /** Returns the TupleTag of this TaggedKeyedPCollection. */
    public TupleTag<V> getTupleTag() {
      return tupleTag;
    }
  }

  /** We use a List to properly track the order in which collections are added. */
  private final List<TaggedKeyedPCollection<K, ?>> keyedCollections;

  private @Nullable Coder<K> keyCoder;

  private final CoGbkResultSchema schema;

  private final Pipeline pipeline;

  KeyedPCollectionTuple(Pipeline pipeline) {
    this(pipeline, new ArrayList<>(), TupleTagList.empty(), null);
  }

  KeyedPCollectionTuple(
      Pipeline pipeline,
      List<TaggedKeyedPCollection<K, ?>> keyedCollections,
      TupleTagList tupleTagList,
      @Nullable Coder<K> keyCoder) {
    this.pipeline = pipeline;
    this.keyedCollections = keyedCollections;
    this.schema = new CoGbkResultSchema(tupleTagList);
    this.keyCoder = keyCoder;
  }

  private static <K> List<TaggedKeyedPCollection<K, ?>> copyAddLast(
      List<TaggedKeyedPCollection<K, ?>> keyedCollections,
      TaggedKeyedPCollection<K, ?> taggedCollection) {
    List<TaggedKeyedPCollection<K, ?>> retval = new ArrayList<>(keyedCollections);
    retval.add(taggedCollection);
    return retval;
  }
}
