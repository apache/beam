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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Flink {@link org.apache.flink.api.common.typeinfo.TypeInformation} for Beam {@link
 * org.apache.beam.sdk.coders.Coder}s.
 */
public class CoderTypeInformation<T> extends TypeInformation<T> implements AtomicType<T> {

  private final Coder<T> coder;
  private final @Nullable SerializablePipelineOptions pipelineOptions;

  public CoderTypeInformation(Coder<T> coder) {
    checkNotNull(coder);
    this.coder = coder;
    this.pipelineOptions = null;
  }

  private CoderTypeInformation(Coder<T> coder, PipelineOptions pipelineOptions) {
    checkNotNull(coder);
    this.coder = coder;
    this.pipelineOptions = new SerializablePipelineOptions(pipelineOptions);
  }

  public Coder<T> getCoder() {
    return coder;
  }

  @Override
  public boolean isBasicType() {
    return false;
  }

  @Override
  public boolean isTupleType() {
    return false;
  }

  @Override
  public int getArity() {
    return 1;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Class<T> getTypeClass() {
    return (Class<T>) coder.getEncodedTypeDescriptor().getRawType();
  }

  @Override
  public boolean isKeyType() {
    return true;
  }

  @Override
  public TypeSerializer<T> createSerializer(ExecutionConfig config) {
    return new CoderTypeSerializer<>(coder, pipelineOptions);
  }

  @Override
  public int getTotalFields() {
    return 2;
  }

  /**
   * Creates a new {@link CoderTypeInformation} with {@link PipelineOptions}, that can be used for
   * {@link org.apache.beam.sdk.io.FileSystems} registration.
   *
   * @see <a href="https://issues.apache.org/jira/browse/BEAM-8577">Jira issue.</a>
   * @param pipelineOptions Options of current pipeline.
   * @return New type information.
   */
  public CoderTypeInformation<T> withPipelineOptions(PipelineOptions pipelineOptions) {
    return new CoderTypeInformation<>(getCoder(), pipelineOptions);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    CoderTypeInformation that = (CoderTypeInformation) o;

    return coder.equals(that.coder);
  }

  @Override
  public int hashCode() {
    return coder.hashCode();
  }

  @Override
  public boolean canEqual(Object obj) {
    return obj instanceof CoderTypeInformation;
  }

  @Override
  public String toString() {
    return "CoderTypeInformation{coder=" + coder + '}';
  }

  @Override
  public TypeComparator<T> createComparator(
      boolean sortOrderAscending, ExecutionConfig executionConfig) {
    throw new UnsupportedOperationException("Non-encoded values cannot be compared directly.");
  }
}
