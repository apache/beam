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
package org.apache.beam.sdk.io.aws2.s3;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.service.AutoService;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.io.FileSystem;
import org.apache.beam.sdk.io.FileSystemRegistrar;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Streams;

/**
 * {@link AutoService} registrar for the {@link S3FileSystem}.
 *
 * <p>Creates instances of {@link S3FileSystem} for each scheme registered with a {@link
 * S3FileSystemSchemeRegistrar}.
 */
@AutoService(FileSystemRegistrar.class)
public class S3FileSystemRegistrar implements FileSystemRegistrar {

  @Override
  public Iterable<FileSystem<?>> fromOptions(@Nonnull PipelineOptions options) {
    checkNotNull(options, "Expect the runner have called FileSystems.setDefaultPipelineOptions().");
    Map<String, FileSystem<?>> fileSystems =
        Streams.stream(
                ServiceLoader.load(
                    S3FileSystemSchemeRegistrar.class, ReflectHelpers.findClassLoader()))
            .flatMap(r -> Streams.stream(r.fromOptions(options)))
            .collect(Collectors.toMap(S3FileSystemConfiguration::getScheme, S3FileSystem::new));
    return fileSystems.values();
  }
}
