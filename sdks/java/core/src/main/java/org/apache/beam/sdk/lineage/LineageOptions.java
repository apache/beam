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
package org.apache.beam.sdk.lineage;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Pipeline options for selecting a custom {@link LineageBase} implementation.
 *
 * <p>When not set, the default Metrics-based lineage is used. Can be set from the command line:
 * {@code --lineageType=com.example.MyLineage}
 */
public interface LineageOptions extends PipelineOptions {

  @Description(
      "The fully qualified class name of the LineageBase implementation to use for recording "
          + "lineage. The class must implement LineageBase and have a public constructor accepting "
          + "(PipelineOptions, Lineage.LineageDirection). "
          + "If not specified, the default Metrics-based lineage is used.")
  @Nullable
  Class<? extends LineageBase> getLineageType();

  void setLineageType(@Nullable Class<? extends LineageBase> lineageClass);
}
