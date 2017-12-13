/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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
package cz.seznam.euphoria.hadoop.input;

import cz.seznam.euphoria.core.client.io.BoundedDataSource;
import cz.seznam.euphoria.core.client.io.BoundedReader;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.util.ExceptionUtils;
import cz.seznam.euphoria.hadoop.SerializableWritable;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Base class for all hadoop based sources.
 *
 * @param <K> type of key handled by the source
 * @param <V> type of value handled by the source
 */
public class HadoopSource<K, V>
    implements BoundedDataSource<Pair<K, V>> {

  private final Class<K> keyClass;
  private final Class<V> valueClass;
  private final Class<? extends InputFormat<K, V>> inputFormatClass;
  private final SerializableWritable<Configuration> conf;

  protected HadoopSource(
      Class<K> keyClass,
      Class<V> valueClass,
      Class<? extends InputFormat<K, V>> inputFormatClass,
      Configuration conf) {
    this.keyClass = Objects.requireNonNull(keyClass);
    this.valueClass = Objects.requireNonNull(valueClass);
    this.inputFormatClass = Objects.requireNonNull(inputFormatClass);
    this.conf = new SerializableWritable<>(Objects.requireNonNull(conf));
  }

  @Override
  public List<BoundedDataSource<Pair<K, V>>> split(long desiredSplitSizeBytes) {
    return doSplit(newJob());
  }

  protected List<BoundedDataSource<Pair<K, V>>> doSplit(Job job) {
    return ExceptionUtils.unchecked(() -> newInputFormatClass()
        .getSplits(job)
        .stream()
        .map(split -> {
          System.out.println(split.toString());
          return new HadoopSplit<>(this, split);
        })
        .collect(Collectors.toList()));
  }

  /**
   * Get class type of key.
   * @return key class
   */
  public Class<K> getKeyClass() {
    return keyClass;
  }

  /**
   * Get class type of value.
   * @return value class
   */
  public Class<V> getValueClass() {
    return valueClass;
  }

  protected InputFormat<K, V> newInputFormatClass() {
    return ExceptionUtils.unchecked(() -> {
      final InputFormat<K, V> inputFormat = inputFormatClass.newInstance();
      if (inputFormat instanceof Configurable) {
        ((Configurable) inputFormat).setConf(conf.get());
      }
      return inputFormat;
    });
  }

  protected Configuration getConf() {
    return conf.get();
  }

  protected Job newJob() {
    return ExceptionUtils.unchecked(() -> Job.getInstance(getConf()));
  }

  @Override
  public BoundedReader<Pair<K, V>> openReader() throws IOException {
    throw new UnsupportedOperationException("Please call `split` on this source first.");
  }

  @Override
  public Set<String> getLocations() {
    return Collections.singleton("unknown");
  }
}
