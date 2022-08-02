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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.io.FileSystem;
import org.apache.beam.sdk.io.FileSystemRegistrar;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configuration;

/** {@link AutoService} registrar for the {@link HadoopFileSystem}. */
@AutoService(FileSystemRegistrar.class)
@Experimental(Kind.FILESYSTEM)
public class HadoopFileSystemRegistrar implements FileSystemRegistrar {

  private static final List<String> HA_SCHEMES = Arrays.asList("hdfs", "webhdfs");

  // Using hard-coded value to avoid incompatibility between HDFS client
  // (org.apache.hadoop:hadoop-dfs-client) version 2.7's DFSConfigKeys and version 2.8's
  // HdfsClientConfigKeys.
  private static final String CONFIG_KEY_DFS_NAMESERVICES = "dfs.nameservices";

  @Override
  public Iterable<FileSystem<?>> fromOptions(@Nonnull PipelineOptions options) {
    final List<Configuration> configurations =
        options.as(HadoopFileSystemOptions.class).getHdfsConfiguration();
    if (configurations == null) {
      // nothing to register
      return Collections.emptyList();
    }
    checkArgument(
        configurations.size() == 1,
        String.format(
            "The %s currently only supports at most a single Hadoop configuration.",
            HadoopFileSystemRegistrar.class.getSimpleName()));

    final ImmutableList.Builder<FileSystem<?>> builder = ImmutableList.builder();
    final Set<String> registeredSchemes = new HashSet<>();

    // this will only do zero or one loop
    final Configuration configuration = Iterables.getOnlyElement(configurations);
    final String defaultFs = configuration.get(org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY);
    if (defaultFs != null && !defaultFs.isEmpty()) {
      final String scheme =
          Objects.requireNonNull(
              URI.create(defaultFs).getScheme(),
              String.format(
                  "Empty scheme for %s value.",
                  org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY));
      builder.add(new HadoopFileSystem(scheme, configuration));
      registeredSchemes.add(scheme);
    }
    final String nameServices = configuration.get(CONFIG_KEY_DFS_NAMESERVICES);
    if (nameServices != null && !nameServices.isEmpty()) {
      // we can register schemes that are support by HA cluster
      for (String scheme : HA_SCHEMES) {
        if (!registeredSchemes.contains(scheme)) {
          builder.add(new HadoopFileSystem(scheme, configuration));
        }
      }
    }
    return builder.build();
  }
}
