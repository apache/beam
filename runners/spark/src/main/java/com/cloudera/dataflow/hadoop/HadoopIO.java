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

    public static <K, V> Bound<K, V> from(String filepattern, Class<? extends FileInputFormat<K, V>> format,
        Class<K> key, Class<V> value) {
      return new Bound<>(filepattern, format, key, value);
    }

    public static class Bound<K, V> extends PTransform<PInput, PCollection<KV<K, V>>> {

      private final String filepattern;
      private final Class<? extends FileInputFormat<K, V>> formatClass;
      private final Class<K> keyClass;
      private final Class<V> valueClass;

      Bound(String filepattern, Class<? extends FileInputFormat<K, V>> format, Class<K> key,
          Class<V> value) {
        Preconditions.checkNotNull(filepattern,
                                   "need to set the filepattern of an HadoopIO.Read transform");
        Preconditions.checkNotNull(format,
                                   "need to set the format class of an HadoopIO.Read transform");
        Preconditions.checkNotNull(key,
                                   "need to set the key class of an HadoopIO.Read transform");
        Preconditions.checkNotNull(value,
                                   "need to set the value class of an HadoopIO.Read transform");
        this.filepattern = filepattern;
        this.formatClass = format;
        this.keyClass = key;
        this.valueClass = value;
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
        return PCollection.createPrimitiveOutputInternal(input.getPipeline(),
            WindowingStrategy.globalDefault(), PCollection.IsBounded.BOUNDED);
      }

    }

  }
}
