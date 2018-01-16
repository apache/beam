/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.hadoop.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import javax.annotation.Nullable;

/**
 * A convenience data sink to produce output through hadoop's sequence file for a
 * specified key and value type.
 *
 * @param <K> the type of the keys emitted
 * @param <V> the type of the values emitted
 */
public class SequenceFileSink<K, V> extends HadoopSink<K, V> {

  /**
   * @param keyClass the class representing the type of the keys emitted
   * @param valueClass the class representing the type of the values emitted
   * @param <K> type of the keys emitted
   * @param <V> type of the values emitted
   * @return OutputPathBuilder
   */
  public static <K, V> OutputPathBuilder<K, V> of(Class<K> keyClass, Class<V> valueClass) {
    return new OutputPathBuilder<>(keyClass, valueClass);
  }

  public static class OutputPathBuilder<K, V> {

    private final Class<K> keyClass;
    private final Class<V> valueClass;

    OutputPathBuilder(Class<K> keyClass, Class<V> valueClass) {
      this.keyClass = keyClass;
      this.valueClass = valueClass;
    }

    /**
     * @param outputPath the destination where to save the output
     * @return Builder with optional setters
     */
    public WithConfigurationBuilder<K, V> outputPath(String outputPath) {
      return new WithConfigurationBuilder<>(keyClass, valueClass, outputPath);
    }
  }

  public static class WithConfigurationBuilder<K, V> {

    private final Class<K> keyClass;
    private final Class<V> valueClass;
    private final String outputPath;

    WithConfigurationBuilder(
        Class<K> keyClass,
        Class<V> valueType,
        String outputPath) {
      this.keyClass = keyClass;
      this.valueClass = valueType;
      this.outputPath = outputPath;
    }

    /**
     * Optional setter if not used it will be created new hadoop configuration.
     * @param configuration the hadoop configuration to build on top of
     * @return Builder
     */
    public WithCompressionBuilder<K, V> withConfiguration(Configuration configuration) {
      return new WithCompressionBuilder<>(keyClass, valueClass, outputPath, configuration);
    }

    /**
     * Optional setter for compression
     * @param compressionClass COMPRESS_CODEC class
     * @param compressionType COMPRESS_TYPE value
     * @return Builder
     */
    public SinkBuilder<K, V> withCompression(
        Class<? extends CompressionCodec> compressionClass,
        SequenceFile.CompressionType compressionType) {
      return withConfiguration(null).withCompression(compressionClass, compressionType);
    }

    public SequenceFileSink<K, V> build() {
      return withCompression(null, null).build();
    }
  }

  public static class WithCompressionBuilder<K, V> {

    private final Class<K> keyType;
    private final Class<V> valueType;
    private final String outputPath;
    @Nullable
    private final Configuration configuration;

    WithCompressionBuilder(
        Class<K> keyType,
        Class<V> valueType,
        String outputPath,
        @Nullable Configuration configuration) {
      this.keyType = keyType;
      this.valueType = valueType;
      this.outputPath = outputPath;
      this.configuration = configuration;
    }

    /**
     * Optional setter for compression
     * @param compressionClass COMPRESS_CODEC class
     * @param compressionType COMPRESS_TYPE value
     * @return Builder
     */
    public SinkBuilder<K, V> withCompression(
        Class<? extends CompressionCodec> compressionClass,
        SequenceFile.CompressionType compressionType) {
      return new SinkBuilder<>(
          keyType,
          valueType,
          outputPath,
          configuration,
          compressionClass,
          compressionType);
    }

    public SequenceFileSink<K, V> build() {
      return withCompression(null, null).build();
    }
  }


  public static class SinkBuilder<K, V> {

    private final Class<K> keyClass;
    private final Class<V> valueType;
    private final String outputPath;
    @Nullable
    private final Configuration configuration;
    @Nullable
    private final Class<? extends CompressionCodec> compressionClass;
    @Nullable
    private final SequenceFile.CompressionType compressionType;

    SinkBuilder(
        Class<K> keyClass,
        Class<V> valueType,
        String outputPath,
        @Nullable Configuration configuration,
        @Nullable Class<? extends CompressionCodec> compressionClass,
        @Nullable SequenceFile.CompressionType compressionType
    ) {

      this.keyClass = keyClass;
      this.valueType = valueType;
      this.outputPath = outputPath;
      this.configuration = configuration;
      this.compressionClass = compressionClass;
      this.compressionType = compressionType;
    }

    public SequenceFileSink<K, V> build() {
      final Configuration newConfiguration = configuration == null
          ? new Configuration()
          : new Configuration(configuration);

      if (compressionClass != null && compressionType != null) {
        newConfiguration.setBoolean(
            FileOutputFormat.COMPRESS, true);
        newConfiguration.set(
            FileOutputFormat.COMPRESS_TYPE, compressionType.toString());
        newConfiguration.setClass(
            FileOutputFormat.COMPRESS_CODEC, compressionClass, CompressionCodec.class);

      }

      return new SequenceFileSink<>(
          keyClass,
          valueType,
          outputPath,
          newConfiguration);
    }
  }


  /**
   * Convenience constructor delegating to
   * {@link #SequenceFileSink(Class, Class, String, Configuration)} with a newly
   * created hadoop configuration.
   *
   * @param keyType the type of the key consumed and emitted to the output
   *         by the new data sink
   * @param valueType the type of the value consumed and emitted to the output
   *         by the new data sink
   * @param path the destination where to place the output to
   *
   * @deprecated will be in next release private, use {@link #of(Class, Class)} instead
   */
  public SequenceFileSink(Class<K> keyType, Class<V> valueType, String path) {
    this(keyType, valueType, path, new Configuration());
  }

  /**
   *
   * Constructs a data sink based on hadoop's {@link SequenceFileOutputFormat}.
   * The specified path is automatically set/overridden in the given hadoop
   * configuration as well as the key and value types.
   *
   * @param keyType the class representing the type of the keys emitted
   * @param valueType the class representing the type of the values emitted
   * @param path the destination where to save the output
   * @param hadoopConfig the hadoop configuration to build on top of
   *
   * @throws NullPointerException if any of the parameters is {@code null}
   *
   * @deprecated will be in next release private, use {@link #of(Class, Class)} instead
   */
  public SequenceFileSink(Class<K> keyType, Class<V> valueType,
                          String path, Configuration hadoopConfig) {
    super((Class) SequenceFileOutputFormat.class,
        wrap(hadoopConfig, path, keyType.getName(), valueType.getName()));
  }

  private static Configuration wrap(Configuration conf,
                                    String path,
                                    String keyTypeName,
                                    String valueTypeName) {
    final Configuration wrap = new Configuration(conf);
    wrap.set(FileOutputFormat.OUTDIR, path);
    wrap.set(JobContext.OUTPUT_KEY_CLASS, keyTypeName);
    wrap.set(JobContext.OUTPUT_VALUE_CLASS, valueTypeName);
    return wrap;
  }
}
