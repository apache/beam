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
package org.apache.beam.runners.flink.translation.types;

import java.io.EOFException;
import java.io.IOException;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.translation.wrappers.DataInputViewWrapper;
import org.apache.beam.runners.flink.translation.wrappers.DataOutputViewWrapper;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Flink {@link org.apache.flink.api.common.typeutils.TypeSerializer} for Beam {@link
 * org.apache.beam.sdk.coders.Coder Coders}.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class CoderTypeSerializer<T> extends TypeSerializer<T> {

  private static final long serialVersionUID = 7247319138941746449L;

  private final Coder<T> coder;

  private final boolean fasterCopy;

  public CoderTypeSerializer(Coder<T> coder, SerializablePipelineOptions pipelineOptions) {
    this(
        coder,
        Preconditions.checkNotNull(pipelineOptions)
            .get()
            .as(FlinkPipelineOptions.class)
            .getFasterCopy());
  }

  public CoderTypeSerializer(Coder<T> coder, boolean fasterCopy) {
    Preconditions.checkNotNull(coder);
    this.coder = coder;
    this.fasterCopy = fasterCopy;
  }

  @Override
  public boolean isImmutableType() {
    return false;
  }

  @Override
  public CoderTypeSerializer<T> duplicate() {
    return new CoderTypeSerializer<>(coder, fasterCopy);
  }

  @Override
  public T createInstance() {
    return null;
  }

  @Override
  public T copy(T t) {
    if (fasterCopy) {
      return t;
    }
    try {
      return CoderUtils.clone(coder, t);
    } catch (CoderException e) {
      throw new RuntimeException("Could not clone.", e);
    }
  }

  @Override
  public T copy(T t, T reuse) {
    return copy(t);
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public void serialize(T t, DataOutputView dataOutputView) throws IOException {
    DataOutputViewWrapper outputWrapper = new DataOutputViewWrapper(dataOutputView);
    coder.encode(t, outputWrapper);
  }

  @Override
  public T deserialize(DataInputView dataInputView) throws IOException {
    try {
      DataInputViewWrapper inputWrapper = new DataInputViewWrapper(dataInputView);
      return coder.decode(inputWrapper);
    } catch (CoderException e) {
      Throwable cause = e.getCause();
      if (cause instanceof EOFException) {
        throw (EOFException) cause;
      } else {
        throw e;
      }
    }
  }

  @Override
  public T deserialize(T t, DataInputView dataInputView) throws IOException {
    return deserialize(dataInputView);
  }

  @Override
  public void copy(DataInputView dataInputView, DataOutputView dataOutputView) throws IOException {
    serialize(deserialize(dataInputView), dataOutputView);
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    CoderTypeSerializer that = (CoderTypeSerializer) o;
    return coder.equals(that.coder);
  }

  @Override
  public int hashCode() {
    return coder.hashCode();
  }

  @Override
  public TypeSerializerSnapshot<T> snapshotConfiguration() {
    return new UnversionedTypeSerializerSnapshot<>(this);
  }

  @Override
  public String toString() {
    return "CoderTypeSerializer{" + "coder=" + coder + '}';
  }
}
