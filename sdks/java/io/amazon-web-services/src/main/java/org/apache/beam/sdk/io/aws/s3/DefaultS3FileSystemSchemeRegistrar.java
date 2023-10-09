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
package org.apache.beam.sdk.io.aws.s3;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.service.AutoService;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.io.aws.options.S3Options;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

/** Registers the "s3" uri schema to be handled by {@link S3FileSystem}. */
@AutoService(S3FileSystemSchemeRegistrar.class)
public class DefaultS3FileSystemSchemeRegistrar implements S3FileSystemSchemeRegistrar {

  @Override
  public Iterable<S3FileSystemConfiguration> fromOptions(@Nonnull PipelineOptions options) {
    checkNotNull(options, "Expect the runner have called FileSystems.setDefaultPipelineOptions().");
    return ImmutableList.of(
        S3FileSystemConfiguration.fromS3Options(options.as(S3Options.class)).build());
  }
}
