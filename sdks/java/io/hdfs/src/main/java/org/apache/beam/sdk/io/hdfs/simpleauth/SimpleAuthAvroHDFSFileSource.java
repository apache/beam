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
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.hdfs.AvroHDFSFileSource;
import org.apache.beam.sdk.io.hdfs.HDFSFileSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * Source for Avros on Hadoop/HDFS with Simple Authentication.
 *
 * <p>Allows to set arbitrary username as HDFS user, which is used for reading Avro from HDFS.
 */
public class SimpleAuthAvroHDFSFileSource<T> extends AvroHDFSFileSource<T> {
  // keep this field to pass Hadoop user between workers
  private final String username;

  /**
   * Create a {@code SimpleAuthAvroHDFSFileSource} based on a file or a file pattern specification.
   * @param username HDFS username.
   */
  public SimpleAuthAvroHDFSFileSource(String filepattern,
                                      AvroCoder<T> avroCoder,
                                      String username) {
    super(filepattern, avroCoder);
    this.username = username;
  }

  /**
   * Create a {@code SimpleAuthAvroHDFSFileSource} based on a single Hadoop input split, which won't
   * be split up further.
   * @param username HDFS username.
   */
  public SimpleAuthAvroHDFSFileSource(String filepattern,
                                      AvroCoder<T> avroCoder,
                                      HDFSFileSource.SerializableSplit serializableSplit,
                                      String username) {
    super(filepattern, avroCoder, serializableSplit);
    this.username = username;
  }

  @Override
  public List<? extends AvroHDFSFileSource<T>> splitIntoBundles(long desiredBundleSizeBytes,
                                                                PipelineOptions options)
      throws Exception {
    if (serializableSplit == null) {
      return Lists.transform(computeSplits(desiredBundleSizeBytes),
          new Function<InputSplit, AvroHDFSFileSource<T>>() {
            @Override
            public AvroHDFSFileSource<T> apply(@Nullable InputSplit inputSplit) {
              return new SimpleAuthAvroHDFSFileSource<>(
                  filepattern, avroCoder, new HDFSFileSource.SerializableSplit(inputSplit),
                  username);
            }
          });
    } else {
      return ImmutableList.of(this);
    }
  }
}
