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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
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
   *
   * @param keyType the class representing the type of the keys emitted
   * @param valueType the class representing the type of the values emitted
   * @param <K> type of the keys emitted
   * @param <V> type of the values emitted
   * @return Builder
   */
  public static <K, V> Builder<K, V> newBuilder(Class<K> keyType, Class<V> valueType) {
    return new Builder<>(keyType, valueType);
  }

  public static class Builder<K, V> {
    Class<K> keyType = null;
    Class<V> valueType = null;
    Configuration conf = new Configuration();
    String outputPath = null;

    Builder(Class<K> keyType, Class<V> valueType) {
      this.keyType = keyType;
      this.valueType = valueType;
    }

    /**
     * Optional setter for compression
     * @param compressionClass COMPRESS_CODEC class
     * @param compressionType COMPRESS_TYPE value
     * @return Builder
     */
    public Builder<K, V> setCompression(Class compressionClass, String compressionType) {
      conf.setBoolean(FileOutputFormat.COMPRESS, true);
      conf.set(FileOutputFormat.COMPRESS_TYPE, compressionType);
      conf.setClass(FileOutputFormat.COMPRESS_CODEC, compressionClass, CompressionCodec.class);
      return this;
    }

    /**
     * Optional setter for {@link Lz4Codec} compression with block {@link SequenceFile.CompressionType}
     * @return Builder
     */
    public Builder<K, V> setLz4Compression() {
      conf.setBoolean(FileOutputFormat.COMPRESS, true);
      conf.set(FileOutputFormat.COMPRESS_TYPE, SequenceFile.CompressionType.BLOCK.toString());
      conf.setClass(FileOutputFormat.COMPRESS_CODEC, Lz4Codec.class, CompressionCodec.class);
      return this;
    }

    /**
     * Optional setter if not used it will be created new hadoop configuration.
     * @param configuration the hadoop configuration to build on top of
     * @return Builder
     */
    public Builder<K,V> setConfiguration(Configuration configuration){
      this.conf = configuration;
      return this;
    }

    /**
     * @param outputPath the destination where to save the output
     * @return Builder
     */
    public Builder<K, V> setOutputPath(String outputPath) {
      this.outputPath = outputPath;
      return this;
    }

    public SequenceFileSink<K, V> build() {
      Preconditions.checkArgument(keyType != null, "Specify type of the Key in `newBuilder`");
      Preconditions.checkArgument(valueType != null,
          "Specify type of the Value in `newBuilder`");
      Preconditions.checkArgument(outputPath != null,
          "Specify type of the Value by call to `setOutputPath`");

      return new SequenceFileSink<>(keyType, valueType, outputPath, conf);
    }
  }

  /**
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
   */
  @SuppressWarnings("unchecked")
  private SequenceFileSink(Class<K> keyType, Class<V> valueType,
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
