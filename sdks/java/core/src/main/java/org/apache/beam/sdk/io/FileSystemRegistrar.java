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
package org.apache.beam.sdk.io;

import com.google.auto.service.AutoService;
import java.util.ServiceLoader;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * A registrar that creates {@link FileSystem} instances from {@link PipelineOptions}.
 *
 * <p>{@link FileSystem} creators have the ability to provide a registrar by creating a {@link
 * ServiceLoader} entry and a concrete implementation of this interface.
 *
 * <p>It is optional but recommended to use one of the many build time tools such as {@link
 * AutoService} to generate the necessary META-INF files automatically.
 */
@Experimental(Kind.FILESYSTEM)
public interface FileSystemRegistrar {
  /**
   * Create zero or more {@link FileSystem filesystems} from the given {@link PipelineOptions}.
   *
   * <p>Each {@link FileSystem#getScheme() scheme} is required to be unique among all {@link
   * FileSystem}s registered by all {@link FileSystemRegistrar}s.
   */
  Iterable<FileSystem> fromOptions(PipelineOptions options);
}
