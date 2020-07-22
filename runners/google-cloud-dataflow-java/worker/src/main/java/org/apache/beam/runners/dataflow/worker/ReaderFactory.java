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
package org.apache.beam.runners.dataflow.worker;

import com.google.auto.service.AutoService;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Creates a {@link NativeReader} from a Dataflow API source definition, presented as a {@link
 * CloudObject}.
 */
public interface ReaderFactory {

  /**
   * A {@link ServiceLoader} interface used to install factories into the {@link
   * ReaderRegistry#defaultRegistry() default ReaderRegistry}.
   *
   * <p>It is optional but recommended to use one of the many build time tools such as {@link
   * AutoService} to generate the necessary META-INF files automatically.
   */
  interface Registrar {

    /**
     * Returns a mapping from a well known type to a {@link ReaderRegistry} capable of instantiating
     * a {@link NativeReader}
     *
     * <p>Each well known type is required to be unique among all registered reader factory
     * registrars.
     */
    Map<String, ReaderFactory> factories();
  }

  /**
   * Creates a {@link NativeReader} from a Dataflow API source definition, presented as a {@link
   * CloudObject}.
   *
   * @throws Exception if a {@link NativeReader} could not be created
   */
  NativeReader<?> create(
      CloudObject cloudSourceSpec,
      @Nullable Coder<?> coder,
      @Nullable PipelineOptions options,
      @Nullable DataflowExecutionContext executionContext,
      DataflowOperationContext context)
      throws Exception;
}
