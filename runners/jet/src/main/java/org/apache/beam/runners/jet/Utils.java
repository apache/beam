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
package org.apache.beam.runners.jet;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static java.util.stream.Collectors.toList;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.core.construction.TransformInputs;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Various common methods used by the Jet based runner. */
public class Utils {

  public static String getTupleTagId(PValue value) {
    Map<TupleTag<?>, PValue> expansion = value.expand();
    return Iterables.getOnlyElement(expansion.keySet()).getId();
  }

  static PValue getMainInput(Pipeline pipeline, TransformHierarchy.Node node) {
    Collection<PValue> mainInputs = getMainInputs(pipeline, node);
    return mainInputs == null ? null : Iterables.getOnlyElement(mainInputs);
  }

  static Collection<PValue> getMainInputs(Pipeline pipeline, TransformHierarchy.Node node) {
    if (node.getTransform() == null) {
      return null;
    }
    return TransformInputs.nonAdditionalInputs(node.toAppliedPTransform(pipeline));
  }

  static Map<TupleTag<?>, PValue> getInputs(AppliedPTransform<?, ?, ?> appliedTransform) {
    return appliedTransform.getInputs();
  }

  static Map<TupleTag<?>, PValue> getAdditionalInputs(TransformHierarchy.Node node) {
    return node.getTransform() != null ? node.getTransform().getAdditionalInputs() : null;
  }

  @SuppressWarnings("unchecked")
  static PValue getInput(AppliedPTransform<?, ?, ?> appliedTransform) {
    if (appliedTransform.getTransform() == null) {
      return null;
    }
    return Iterables.getOnlyElement(TransformInputs.nonAdditionalInputs(appliedTransform));
  }

  static Map<TupleTag<?>, PValue> getOutputs(AppliedPTransform<?, ?, ?> appliedTransform) {
    if (appliedTransform.getTransform() == null) {
      return null;
    }
    return appliedTransform.getOutputs();
  }

  static Map.Entry<TupleTag<?>, PValue> getOutput(AppliedPTransform<?, ?, ?> appliedTransform) {
    return Iterables.getOnlyElement(getOutputs(appliedTransform).entrySet());
  }

  static <T> boolean isBounded(AppliedPTransform<?, ?, ?> appliedTransform) {
    return ((PCollection) getOutput(appliedTransform).getValue())
        .isBounded()
        .equals(PCollection.IsBounded.BOUNDED);
  }

  static boolean isKeyedValueCoder(Coder coder) {
    if (coder instanceof KvCoder) {
      return true;
    } else if (coder instanceof WindowedValue.WindowedValueCoder) {
      return ((WindowedValue.WindowedValueCoder) coder).getValueCoder() instanceof KvCoder;
    }
    return false;
  }

  static Coder getCoder(PCollection pCollection) {
    if (pCollection.getWindowingStrategy() == null) {
      return pCollection.getCoder();
    } else {
      return getWindowedValueCoder(pCollection);
    }
  }

  static <T> WindowedValue.WindowedValueCoder<T> getWindowedValueCoder(PCollection<T> pCollection) {
    return WindowedValue.FullWindowedValueCoder.of(
        pCollection.getCoder(), pCollection.getWindowingStrategy().getWindowFn().windowCoder());
  }

  static <T> Map<T, Coder> getCoders(
      Map<TupleTag<?>, PValue> pCollections,
      Function<Map.Entry<TupleTag<?>, PValue>, T> tupleTagExtractor) {
    return pCollections.entrySet().stream()
        .collect(
            Collectors.toMap(
                tupleTagExtractor, e -> getCoder((PCollection) e.getValue()), (v1, v2) -> v1));
  }

  static Map<TupleTag<?>, Coder<?>> getOutputValueCoders(
      AppliedPTransform<?, ?, ?> appliedTransform) {
    return appliedTransform.getOutputs().entrySet().stream()
        .filter(e -> e.getValue() instanceof PCollection)
        .collect(Collectors.toMap(Map.Entry::getKey, e -> ((PCollection) e.getValue()).getCoder()));
  }

  static Collection<PCollectionView<?>> getSideInputs(AppliedPTransform<?, ?, ?> appliedTransform) {
    PTransform<?, ?> transform = appliedTransform.getTransform();
    if (transform instanceof ParDo.MultiOutput) {
      ParDo.MultiOutput multiParDo = (ParDo.MultiOutput) transform;
      return (List) multiParDo.getSideInputs().values().stream().collect(Collectors.toList());
    } else if (transform instanceof ParDo.SingleOutput) {
      ParDo.SingleOutput singleParDo = (ParDo.SingleOutput) transform;
      return (List) singleParDo.getSideInputs().values().stream().collect(Collectors.toList());
    }
    return Collections.emptyList();
  }

  static boolean usesStateOrTimers(AppliedPTransform<?, ?, ?> appliedTransform) {
    try {
      return ParDoTranslation.usesStateOrTimers(appliedTransform);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static DoFn<?, ?> getDoFn(AppliedPTransform<?, ?, ?> appliedTransform) {
    try {
      DoFn<?, ?> doFn = ParDoTranslation.getDoFn(appliedTransform);
      if (DoFnSignatures.isSplittable(doFn)) {
        throw new IllegalStateException(
            "Not expected to directly translate splittable DoFn, should have been overridden: "
                + doFn); // todo
      }
      if (DoFnSignatures.requiresTimeSortedInput(doFn)) {
        throw new UnsupportedOperationException(
            String.format(
                "%s doesn't currently support @RequiresTimeSortedInput annotation.",
                JetRunner.class.getSimpleName()));
      }
      return doFn;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static WindowingStrategy<?, ?> getWindowingStrategy(AppliedPTransform<?, ?, ?> appliedTransform) {
    // assume that the windowing strategy is the same for all outputs

    Map<TupleTag<?>, PValue> outputs = getOutputs(appliedTransform);

    if (outputs == null || outputs.isEmpty()) {
      throw new IllegalStateException("No outputs defined.");
    }

    PValue taggedValue = outputs.values().iterator().next();
    checkState(
        taggedValue instanceof PCollection,
        "Within ParDo, got a non-PCollection output %s of type %s",
        taggedValue,
        taggedValue.getClass().getSimpleName());
    PCollection<?> coll = (PCollection<?>) taggedValue;
    return coll.getWindowingStrategy();
  }

  /**
   * Assigns the {@code list} to {@code count} sublists in a round-robin fashion. One call returns
   * the {@code index}-th sublist.
   *
   * <p>For example, for a 7-element list where {@code count == 3}, it would respectively return for
   * indices 0..2:
   *
   * <pre>
   *   0, 3, 6
   *   1, 4
   *   2, 5
   * </pre>
   */
  @Nonnull
  public static <T> List<T> roundRobinSubList(@Nonnull List<T> list, int index, int count) {
    if (index < 0 || index >= count) {
      throw new IllegalArgumentException("index=" + index + ", count=" + count);
    }
    return IntStream.range(0, list.size())
        .filter(i -> i % count == index)
        .mapToObj(list::get)
        .collect(toList());
  }

  /** Returns a deep clone of an object by serializing and deserializing it (ser-de). */
  @SuppressWarnings("unchecked")
  public static <T> T serde(T object) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(object);
      oos.close();
      byte[] byteData = baos.toByteArray();
      ByteArrayInputStream bais = new ByteArrayInputStream(byteData);
      return (T) new ObjectInputStream(bais).readObject();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static <T> byte[] encode(T value, Coder<T> coder) {
    try {
      return CoderUtils.encodeToByteArray(coder, value);
    } catch (IOException e) {
      throw rethrow(e);
    }
  }

  public static <T> WindowedValue<T> decodeWindowedValue(byte[] item, Coder coder) {
    try {
      return (WindowedValue<T>) CoderUtils.decodeFromByteArray(coder, item);
    } catch (IOException e) {
      throw rethrow(e);
    }
  }

  public static WindowedValue.FullWindowedValueCoder deriveIterableValueCoder(
      WindowedValue.FullWindowedValueCoder elementCoder) {
    return WindowedValue.FullWindowedValueCoder.of(
        ListCoder.of(elementCoder.getValueCoder()), elementCoder.getWindowCoder());
  }

  /** A wrapper of {@code byte[]} that can be used as a hash-map key. */
  public static class ByteArrayKey {
    private final byte[] value;
    private int hash;

    public ByteArrayKey(@Nonnull byte[] value) {
      this.value = value;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ByteArrayKey that = (ByteArrayKey) o;
      return Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
      if (hash == 0) {
        hash = Arrays.hashCode(value);
      }
      return hash;
    }
  }
}
