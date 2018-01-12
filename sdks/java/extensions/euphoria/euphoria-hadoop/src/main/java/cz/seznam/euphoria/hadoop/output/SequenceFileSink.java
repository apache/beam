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

/**
 * A convenience data sink to produce output through hadoop's sequence file for a
 * specified key and value type.
 *
 * @param <K> the type of the keys emitted
 * @param <V> the type of the values emitted
 */
public class SequenceFileSink<K, V> extends HadoopSink<K, V> {

  /**
   * @param keyType the class representing the type of the keys emitted
   * @param valueType the class representing the type of the values emitted
   * @param <K> type of the keys emitted
   * @param <V> type of the values emitted
   * @return OutputPathBuilder
   */
  public static <K, V> OutputPathBuilder<K, V> of(Class<K> keyType, Class<V> valueType) {
    return new OutputPathBuilder<>(keyType, valueType);
  }

  public static class OutputPathBuilder<K, V> {
    private final Class<K> keyType;
    private final Class<V> valueType;
    private String outputPath;

    OutputPathBuilder(Class<K> keyType, Class<V> valueType) {
      this.keyType = keyType;
      this.valueType = valueType;
    }

    /**
     * @param outputPath the destination where to save the output
     * @return Builder with optional setters
     */
    public Builder<K, V> outputPath(String outputPath) {
      this.outputPath = outputPath;
      return new Builder<>(this);
    }
  }

  public static class Builder<K, V> {
    private OutputPathBuilder<K, V> outputPathBuilder;
    private Configuration conf = new Configuration();

    Builder(OutputPathBuilder<K, V> outputPathBuilder) {
      this.outputPathBuilder = outputPathBuilder;
    }

    /**
     * Optional setter for compression
     * @param compressionClass COMPRESS_CODEC class
     * @param compressionType COMPRESS_TYPE value
     * @return Builder
     */
    public Builder<K, V> withCompression(Class<? extends CompressionCodec> compressionClass,
                                         SequenceFile.CompressionType compressionType) {
      conf.setBoolean(FileOutputFormat.COMPRESS, true);
      conf.set(FileOutputFormat.COMPRESS_TYPE, compressionType.toString());
      conf.setClass(FileOutputFormat.COMPRESS_CODEC, compressionClass, CompressionCodec.class);
      return this;
    }

    /**
     * Optional setter if not used it will be created new hadoop configuration.
     * @param configuration the hadoop configuration to build on top of
     * @return Builder
     */
    public Builder<K, V> withConfiguration(Configuration configuration) {
      this.conf = configuration;
      return this;
    }

    public SequenceFileSink<K, V> build() {
      return new SequenceFileSink<>(
          outputPathBuilder.keyType,
          outputPathBuilder.valueType,
          outputPathBuilder.outputPath,
          conf);
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
  @SuppressWarnings("unchecked")
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
