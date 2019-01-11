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
package org.apache.beam.runners.spark.io.hadoop;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.ShardNameTemplate;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Spark native HadoopIO.
 */
public final class HadoopIO {

  private HadoopIO() {
  }

  /**
   * Read operation from HDFS.
   */
  public static final class Read {

    private Read() {
    }

    public static <K, V> Bound<K, V> from(String filepattern,
        Class<? extends FileInputFormat<K, V>> format, Class<K> key, Class<V> value) {
      return new Bound<>(filepattern, format, key, value);
    }

    /**
     * A {@link PTransform} reading bounded collection of data from HDFS.
     * @param <K> the type of the keys
     * @param <V> the type of the values
     */
    public static class Bound<K, V> extends PTransform<PBegin, PCollection<KV<K, V>>> {

      private final String filepattern;
      private final Class<? extends FileInputFormat<K, V>> formatClass;
      private final Class<K> keyClass;
      private final Class<V> valueClass;

      Bound(String filepattern, Class<? extends FileInputFormat<K, V>> format, Class<K> key,
          Class<V> value) {
        checkNotNull(filepattern, "need to set the filepattern of an HadoopIO.Read transform");
        checkNotNull(format, "need to set the format class of an HadoopIO.Read transform");
        checkNotNull(key, "need to set the key class of an HadoopIO.Read transform");
        checkNotNull(value, "need to set the value class of an HadoopIO.Read transform");
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
      public PCollection<KV<K, V>> expand(PBegin input) {
        return PCollection.createPrimitiveOutputInternal(input.getPipeline(),
            WindowingStrategy.globalDefault(), PCollection.IsBounded.BOUNDED);
      }

    }

  }

  /**
   * Write operation on HDFS.
   */
  public static final class Write {

    private Write() {
    }

    public static <K, V> Bound<K, V> to(String filenamePrefix,
        Class<? extends FileOutputFormat<K, V>> format, Class<K> key, Class<V> value) {
      return new Bound<>(filenamePrefix, format, key, value);
    }

    /**
     * A {@link PTransform} writing {@link PCollection} on HDFS.
     */
    public static class Bound<K, V> extends PTransform<PCollection<KV<K, V>>, PDone> {

      /** The filename to write to. */
      private final String filenamePrefix;
      /** Suffix to use for each filename. */
      private final String filenameSuffix;
      /** Requested number of shards.  0 for automatic. */
      private final int numShards;
      /** Shard template string. */
      private final String shardTemplate;
      private final Class<? extends FileOutputFormat<K, V>> formatClass;
      private final Class<K> keyClass;
      private final Class<V> valueClass;
      private final Map<String, String> configurationProperties;

      Bound(String filenamePrefix, Class<? extends FileOutputFormat<K, V>> format,
          Class<K> key,
          Class<V> value) {
        this(filenamePrefix, "", 0, ShardNameTemplate.INDEX_OF_MAX, format, key, value,
            new HashMap<String, String>());
      }

      Bound(String filenamePrefix, String filenameSuffix, int numShards,
          String shardTemplate, Class<? extends FileOutputFormat<K, V>> format,
          Class<K> key, Class<V> value, Map<String, String> configurationProperties) {
        this.filenamePrefix = filenamePrefix;
        this.filenameSuffix = filenameSuffix;
        this.numShards = numShards;
        this.shardTemplate = shardTemplate;
        this.formatClass = format;
        this.keyClass = key;
        this.valueClass = value;
        this.configurationProperties = configurationProperties;
      }

      public Bound<K, V> withoutSharding() {
        return new Bound<>(filenamePrefix, filenameSuffix, 1, "", formatClass,
            keyClass, valueClass, configurationProperties);
      }

      public Bound<K, V> withConfigurationProperty(String key, String value) {
        configurationProperties.put(key, value);
        return this;
      }

      public String getFilenamePrefix() {
        return filenamePrefix;
      }

      public String getShardTemplate() {
        return shardTemplate;
      }

      public int getNumShards() {
        return numShards;
      }

      public String getFilenameSuffix() {
        return filenameSuffix;
      }

      public Class<? extends FileOutputFormat<K, V>> getFormatClass() {
        return formatClass;
      }

      public Class<V> getValueClass() {
        return valueClass;
      }

      public Class<K> getKeyClass() {
        return keyClass;
      }

      public Map<String, String> getConfigurationProperties() {
        return configurationProperties;
      }

      @Override
      public PDone expand(PCollection<KV<K, V>> input) {
        checkNotNull(
            filenamePrefix, "need to set the filename prefix of an HadoopIO.Write transform");
        checkNotNull(formatClass, "need to set the format class of an HadoopIO.Write transform");
        checkNotNull(keyClass, "need to set the key class of an HadoopIO.Write transform");
        checkNotNull(valueClass, "need to set the value class of an HadoopIO.Write transform");

        checkArgument(
            ShardNameTemplateAware.class.isAssignableFrom(formatClass),
            "Format class must implement %s",
            ShardNameTemplateAware.class.getName());

        return PDone.in(input.getPipeline());
      }
    }
  }
}
