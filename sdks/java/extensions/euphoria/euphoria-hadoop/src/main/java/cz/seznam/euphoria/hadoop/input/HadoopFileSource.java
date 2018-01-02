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
package cz.seznam.euphoria.hadoop.input;

import cz.seznam.euphoria.core.client.io.BoundedDataSource;
import cz.seznam.euphoria.core.client.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.util.List;

/**
 * A general purpose data source based on top of hadoop input formats.
 *
 * @param <K> the type of record keys
 * @param <V> the type of record values
 */
public class HadoopFileSource<K, V> extends HadoopSource<K, V> {

  /**
   * Split size to use, if we are not satisfied with the one user provided.
   */
  private static final long MIN_SPLIT_SIZE = 128 * 1024 * 1024;

  @SuppressWarnings("unchecked")
  public HadoopFileSource(
      Class<K> keyClass,
      Class<V> valueClass,
      Class<? extends FileInputFormat> inputFormatClass,
      Configuration conf) {
    super(keyClass, valueClass, (Class) inputFormatClass, conf);
  }

  @Override
  public List<BoundedDataSource<Pair<K, V>>> split(long desiredSplitSizeBytes) {
    final Job job = newJob();
    FileInputFormat.setMinInputSplitSize(job, Math.max(MIN_SPLIT_SIZE, desiredSplitSizeBytes));
    FileInputFormat.setMaxInputSplitSize(job, Math.max(MIN_SPLIT_SIZE, desiredSplitSizeBytes));
    return doSplit(job);
  }

}
