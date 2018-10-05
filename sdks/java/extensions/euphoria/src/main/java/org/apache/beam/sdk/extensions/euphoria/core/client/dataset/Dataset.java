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
package org.apache.beam.sdk.extensions.euphoria.core.client.dataset;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeAwareness;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A dataset abstraction.
 *
 * @param <T> type of elements of this data set
 */
@Audience(Audience.Type.CLIENT)
public class Dataset<T> implements PValue {

  public static <T> Dataset<T> of(PCollection<T> pCollection) {
    return new Dataset<>(pCollection, null);
  }

  public static <T> Dataset<T> of(PCollection<T> pCollection, Operator producer) {
    return new Dataset<>(pCollection, producer);
  }

  private final PCollection<T> pCollection;
  @Nullable private final Operator producer;

  private Dataset(PCollection<T> pCollection, @Nullable Operator producer) {
    this.pCollection = pCollection;
    this.producer = producer;
    if (pCollection.getTypeDescriptor() == null) {
      pCollection.setTypeDescriptor(TypeAwareness.orObjects(Optional.empty()));
    }
  }

  @Override
  public String getName() {
    return pCollection.getName();
  }

  /**
   * @deprecated Not intended to be expanded.
   * @return empty map
   */
  @Deprecated
  @Override
  public Map<TupleTag<?>, PValue> expand() {
    return Collections.emptyMap();
  }

  @Override
  public void finishSpecifying(PInput upstreamInput, PTransform<?, ?> upstreamTransform) {
    pCollection.finishSpecifying(upstreamInput, upstreamTransform);
  }

  @Override
  public Pipeline getPipeline() {
    return pCollection.getPipeline();
  }

  @Override
  public void finishSpecifyingOutput(
      String transformName, PInput input, PTransform<?, ?> transform) {
    pCollection.finishSpecifyingOutput(transformName, input, transform);
  }

  /**
   * Get underlying {@link PCollection}.
   *
   * @return pCollection
   */
  public PCollection<T> getPCollection() {
    return pCollection;
  }

  public Optional<Operator> getProducer() {
    return Optional.ofNullable(producer);
  }

  public TypeDescriptor<T> getTypeDescriptor() {
    return pCollection.getTypeDescriptor();
  }
}
