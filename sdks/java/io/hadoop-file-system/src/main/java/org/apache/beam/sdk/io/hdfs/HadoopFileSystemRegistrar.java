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
package org.apache.beam.sdk.io.hdfs;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.io.FileSystem;
import org.apache.beam.sdk.io.FileSystemRegistrar;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.hadoop.conf.Configuration;

/**
 * {@link AutoService} registrar for the {@link HadoopFileSystem}.
 */
@AutoService(FileSystemRegistrar.class)
public class HadoopFileSystemRegistrar implements FileSystemRegistrar {

  @Override
  public Iterable<FileSystem> fromOptions(@Nonnull PipelineOptions options) {
    List<Configuration> configurations =
        options.as(HadoopFileSystemOptions.class).getHdfsConfiguration();
    if (configurations == null) {
      configurations = Collections.emptyList();
    }
    checkArgument(configurations.size() <= 1,
        String.format(
            "The %s currently only supports at most a single Hadoop configuration.",
            HadoopFileSystemRegistrar.class.getSimpleName()));

    ImmutableList.Builder<FileSystem> builder = ImmutableList.builder();
    for (Configuration configuration : configurations) {
      try {
        builder.add(new HadoopFileSystem(configuration));
      } catch (IOException e) {
        throw new IllegalArgumentException(String.format(
            "Failed to construct Hadoop filesystem with configuration %s", configuration), e);
      }
    }
    return builder.build();
  }
}
