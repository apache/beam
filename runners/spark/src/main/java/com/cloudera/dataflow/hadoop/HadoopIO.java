/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.dataflow.hadoop;

import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.common.base.Preconditions;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public final class HadoopIO {

  private HadoopIO() {
  }

  public static final class Read {

    private Read() {
    }

    public static Bound from(String filepattern) {
      return new Bound().from(filepattern);
    }

    public static Bound withFormatClass(Class<? extends FileInputFormat<?, ?>> format) {
      return new Bound().withFormatClass(format);
    }

    public static Bound withKeyClass(Class<?> key) {
      return new Bound().withKeyClass(key);
    }

    public static Bound withValueClass(Class<?> value) {
      return new Bound().withValueClass(value);
    }

    public static class Bound<K, V> extends PTransform<PInput, PCollection<KV<K, V>>> {

      private final String filepattern;
      private final Class<? extends FileInputFormat<K, V>> formatClass;
      private final Class<K> keyClass;
      private final Class<V> valueClass;

      Bound() {
        this(null, null, null, null);
      }

      Bound(String filepattern, Class<? extends FileInputFormat<K, V>> format, Class<K> key,
          Class<V> value) {
        this.filepattern = filepattern;
        this.formatClass = format;
        this.keyClass = key;
        this.valueClass = value;
      }

      public Bound<K, V> from(String file) {
        return new Bound<>(file, formatClass, keyClass, valueClass);
      }

      public Bound<K, V> withFormatClass(Class<? extends FileInputFormat<K, V>> format) {
        return new Bound<>(filepattern, format, keyClass, valueClass);
      }

      public Bound<K, V> withKeyClass(Class<K> key) {
        return new Bound<>(filepattern, formatClass, key, valueClass);
      }

      public Bound<K, V> withValueClass(Class<V> value) {
        return new Bound<>(filepattern, formatClass, keyClass, value);
      }

      public String getFilepattern() {
        return filepattern;
      }

      public Class<? extends FileInputFormat<K, V>> getFormatClass() {
        return formatClass;
      }

      public Class<V> getValueClass() {
        return valueClass;
      }

      public Class<K> getKeyClass() {
        return keyClass;
      }

      @Override
      public PCollection<KV<K, V>> apply(PInput input) {
        Preconditions.checkNotNull(filepattern,
            "need to set the filepattern of an HadoopIO.Read transform");
        Preconditions.checkNotNull(formatClass,
            "need to set the format class of an HadoopIO.Read transform");
        Preconditions.checkNotNull(keyClass,
            "need to set the key class of an HadoopIO.Read transform");
        Preconditions.checkNotNull(valueClass,
            "need to set the value class of an HadoopIO.Read transform");

        return PCollection.createPrimitiveOutputInternal(WindowingStrategy.globalDefault());
      }

    }

  }
}
