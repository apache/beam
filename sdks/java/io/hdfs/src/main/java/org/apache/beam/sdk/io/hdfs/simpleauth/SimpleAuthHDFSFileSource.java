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
package org.apache.beam.sdk.io.hdfs.simpleauth;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.hdfs.HDFSFileSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Source for Hadoop/HDFS with Simple Authentication.
 *
 * <p>Allows to set arbitrary username as HDFS user, which is used for reading from HDFS.
 */
public class SimpleAuthHDFSFileSource<K, V> extends HDFSFileSource<K, V> {
  private final String username;
  /**
   * Create a {@code SimpleAuthHDFSFileSource} based on a single Hadoop input split, which won't be
   * split up further.
   * @param username HDFS username.
   */
  protected SimpleAuthHDFSFileSource(String filepattern,
                                     Class<? extends FileInputFormat<?, ?>> formatClass,
                                     Class<K> keyClass,
                                     Class<V> valueClass,
                                     HDFSFileSource.SerializableSplit serializableSplit,
                                     String username) {
    super(filepattern, formatClass, keyClass, valueClass, serializableSplit);
    this.username = username;
  }

  /**
   * Create a {@code SimpleAuthHDFSFileSource} based on a file or a file pattern specification.
   * @param username HDFS username.
   */
  protected SimpleAuthHDFSFileSource(String filepattern,
                                     Class<? extends FileInputFormat<?, ?>> formatClass,
                                     Class<K> keyClass,
                                     Class<V> valueClass,
                                     String username) {
    super(filepattern, formatClass, keyClass, valueClass);
    this.username = username;
  }

  /**
   * Creates a {@code Read} transform that will read from an {@code SimpleAuthHDFSFileSource}
   * with the given file name or pattern ("glob") using the given Hadoop {@link FileInputFormat},
   * with key-value types specified
   * by the given key class and value class.
   * @param username HDFS username.
   */
  public static <K, V, T extends FileInputFormat<K, V>> Read.Bounded<KV<K, V>> readFrom(
      String filepattern,
      Class<T> formatClass,
      Class<K> keyClass,
      Class<V> valueClass,
      String username) {
    return Read.from(from(filepattern, formatClass, keyClass, valueClass, username));
  }

  /**
   * Creates a {@code SimpleAuthHDFSFileSource} that reads from the given file name or pattern
   * ("glob") using the given Hadoop {@link FileInputFormat}, with key-value types specified by the
   * given key class and value class.
   * @param username HDFS username.
   */
  public static <K, V, T extends FileInputFormat<K, V>> HDFSFileSource<K, V> from(
      String filepattern,
      Class<T> formatClass,
      Class<K> keyClass,
      Class<V> valueClass,
      String username) {
    return new SimpleAuthHDFSFileSource<>(filepattern, formatClass, keyClass, valueClass, username);
  }

  @Override
  public List<? extends BoundedSource<KV<K, V>>> splitIntoBundles(
      long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
    if (serializableSplit == null) {
      return Lists.transform(computeSplits(desiredBundleSizeBytes),
          new Function<InputSplit, BoundedSource<KV<K, V>>>() {
            @Nullable
            @Override
            public BoundedSource<KV<K, V>> apply(@Nullable InputSplit inputSplit) {
              return new SimpleAuthHDFSFileSource<>(filepattern, formatClass, keyClass,
                  valueClass, new HDFSFileSource.SerializableSplit(inputSplit),
                  username);
            }
          });
    } else {
      return ImmutableList.of(this);
    }
  }
}
