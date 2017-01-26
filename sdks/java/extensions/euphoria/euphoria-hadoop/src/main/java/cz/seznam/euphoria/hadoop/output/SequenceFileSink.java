/**
 * Copyright 2016 Seznam a.s.
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

  // TODO provide factory: how to parse out the keyType and valueType info from the URI?

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
   */
  public SequenceFileSink(Class<K> keyType, Class<V> valueType, String path) {
    this(keyType, valueType, path, new Configuration());
  }

  /**
   * Constructs a data sink based on hadoop's {@link SequenceFileOutputFormat}.
   * The specified path is automatically set/overridden in the given hadoop
   * configuration as well as the key and value types.
   *
   * @param path the path to read data from
   * @param hadoopConfig the hadoop configuration to build on top of
   *
   * @throws NullPointerException if any of the parameters is {@code null}
   */
  @SuppressWarnings("unchecked")
  public SequenceFileSink(Class<K> keyType, Class<V> valueType,
                          String path, Configuration hadoopConfig) {
    super((Class) SequenceFileOutputFormat.class, hadoopConfig);
    hadoopConfig.set(FileOutputFormat.OUTDIR, path);
    hadoopConfig.set(JobContext.OUTPUT_KEY_CLASS, keyType.getName());
    hadoopConfig.set(JobContext.OUTPUT_VALUE_CLASS, valueType.getName());
  }
}
