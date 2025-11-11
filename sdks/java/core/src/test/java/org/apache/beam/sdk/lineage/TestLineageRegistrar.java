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

import com.google.auto.service.AutoService;
import org.apache.beam.sdk.metrics.Lineage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A test {@link LineageRegistrar} for ServiceLoader discovery testing.
 *
 * <p>This registrar only activates when {@link TestLineageOptions#getEnableTestLineage()} is true,
 * ensuring it doesn't interfere with other tests in the suite.
 */
@AutoService(LineageRegistrar.class)
public class TestLineageRegistrar implements LineageRegistrar {

  @Override
  public @Nullable Lineage fromOptions(
      PipelineOptions options, Lineage.LineageDirection direction) {
    // Only activate if explicitly enabled via TestLineageOptions
    TestLineageOptions testOptions = options.as(TestLineageOptions.class);
    if (testOptions.getEnableTestLineage()) {
      return new TestLineage(direction);
    }
    // Return null to use default MetricsLineage
    return null;
  }
}
