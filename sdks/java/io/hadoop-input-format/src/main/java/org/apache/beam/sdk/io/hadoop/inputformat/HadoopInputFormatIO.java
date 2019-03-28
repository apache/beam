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
package org.apache.beam.sdk.io.hadoop.inputformat;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.io.hadoop.format.HadoopFormatIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v20_0.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;

/** @deprecated as of version 2.10. Use {@link HadoopFormatIO} instead. */
@Deprecated
@Experimental(Experimental.Kind.SOURCE_SINK)
public class HadoopInputFormatIO {
  /** @deprecated as of version 2.10. Use {@link HadoopFormatIO#read()} instead. */
  @Deprecated
  public static <K, V> Read<K, V> read() {
    return new AutoValue_HadoopInputFormatIO_Read.Builder<K, V>()
        .setHFIORead(HadoopFormatIO.read())
        .build();
  }

  /** @deprecated as of version 2.10. Use {@link HadoopFormatIO} instead. */
  @Deprecated
  @AutoValue
  public abstract static class Read<K, V> extends PTransform<PBegin, PCollection<KV<K, V>>> {

    protected abstract HadoopFormatIO.Read<K, V> getHFIORead();

    /**
     * @deprecated as of version 2.10. Use {@link HadoopFormatIO.Read#getConfiguration()} instead.
     */
    @Deprecated
    @Nullable
    public SerializableConfiguration getConfiguration() {
      return getHFIORead().getConfiguration();
    }

    /**
     * @deprecated as of version 2.10. Use {@link HadoopFormatIO.Read#getKeyTranslationFunction()}
     *     instead.
     */
    @Deprecated
    @Nullable
    public SimpleFunction<?, K> getKeyTranslationFunction() {
      return getHFIORead().getKeyTranslationFunction();
    }

    /**
     * @deprecated as of version 2.10. Use {@link HadoopFormatIO.Read#getValueTranslationFunction()}
     *     instead.
     */
    @Deprecated
    @Nullable
    public SimpleFunction<?, V> getValueTranslationFunction() {
      return getHFIORead().getValueTranslationFunction();
    }

    /**
     * @deprecated as of version 2.10. Use {@link HadoopFormatIO.Read#getKeyTypeDescriptor()}
     *     instead.
     */
    @Deprecated
    @Nullable
    public TypeDescriptor<K> getKeyTypeDescriptor() {
      return getHFIORead().getKeyTypeDescriptor();
    }

    /**
     * @deprecated as of version 2.10. Use {@link HadoopFormatIO.Read#getValueTypeDescriptor()}
     *     instead.
     */
    @Deprecated
    @Nullable
    public TypeDescriptor<V> getValueTypeDescriptor() {
      return getHFIORead().getValueTypeDescriptor();
    }

    /**
     * @deprecated as of version 2.10. Use {@link HadoopFormatIO.Read#getinputFormatClass()}
     *     instead.
     */
    @Deprecated
    @Nullable
    public TypeDescriptor<?> getinputFormatClass() {
      return getHFIORead().getinputFormatClass();
    }

    /**
     * @deprecated as of version 2.10. Use {@link HadoopFormatIO.Read#getinputFormatKeyClass()}
     *     instead.
     */
    @Deprecated
    @Nullable
    public TypeDescriptor<?> getinputFormatKeyClass() {
      return getHFIORead().getinputFormatKeyClass();
    }

    /**
     * @deprecated as of version 2.10. Use {@link HadoopFormatIO.Read#getinputFormatValueClass()}
     *     instead.
     */
    @Deprecated
    @Nullable
    public TypeDescriptor<?> getinputFormatValueClass() {
      return getHFIORead().getinputFormatValueClass();
    }

    abstract Builder<K, V> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<K, V> {
      abstract Builder<K, V> setHFIORead(HadoopFormatIO.Read<K, V> read);

      abstract Read<K, V> build();
    }

    private Read<K, V> withHFIORead(HadoopFormatIO.Read<K, V> kvRead) {
      return toBuilder().setHFIORead(kvRead).build();
    }

    /**
     * @deprecated as of version 2.10. Use {@link
     *     HadoopFormatIO.Read#withConfiguration(Configuration)} instead.
     */
    @Deprecated
    @SuppressWarnings("unchecked")
    public Read<K, V> withConfiguration(Configuration configuration) {
      return withHFIORead(getHFIORead().withConfiguration(configuration));
    }

    /**
     * @deprecated as of version 2.10. Use {@link
     *     HadoopFormatIO.Read#withKeyTranslation(SimpleFunction)} instead.
     */
    @Deprecated
    public Read<K, V> withKeyTranslation(SimpleFunction<?, K> function) {
      return withHFIORead(getHFIORead().withKeyTranslation(function));
    }

    /**
     * @deprecated as of version 2.10. Use {@link
     *     HadoopFormatIO.Read#withValueTranslation(SimpleFunction)} instead.
     */
    @Deprecated
    public Read<K, V> withValueTranslation(SimpleFunction<?, V> function) {
      return withHFIORead(getHFIORead().withValueTranslation(function));
    }

    /** @deprecated as of version 2.10. Use {@link HadoopFormatIO.Read#expand(PBegin)} instead. */
    @Deprecated
    @Override
    public PCollection<KV<K, V>> expand(PBegin input) {
      return getHFIORead().expand(input);
    }

    /**
     * @deprecated as of version 2.10. Use {@link HadoopFormatIO.Read#validateTransform()} instead.
     */
    @Deprecated
    @VisibleForTesting
    void validateTransform() {
      getHFIORead().validateTransform();
    }
  }
}
