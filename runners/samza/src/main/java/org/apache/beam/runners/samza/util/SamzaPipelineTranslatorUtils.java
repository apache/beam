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
package org.apache.beam.runners.samza.util;

import java.io.IOException;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.wire.WireCoders;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.graph.PipelineNode;
import org.apache.beam.sdk.values.PCollection;

/** Utilities for pipeline translation. */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public final class SamzaPipelineTranslatorUtils {
  private SamzaPipelineTranslatorUtils() {}

  public static WindowedValue.WindowedValueCoder instantiateCoder(
      String collectionId, RunnerApi.Components components) {
    PipelineNode.PCollectionNode collectionNode =
        PipelineNode.pCollection(collectionId, components.getPcollectionsOrThrow(collectionId));
    try {
      return (WindowedValue.WindowedValueCoder)
          WireCoders.instantiateRunnerWireCoder(collectionNode, components);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Escape the non-alphabet chars in the name so we can create a physical stream out of it.
   *
   * <p>This escape will replace any non-alphanumeric characters other than "-" and "_" with "_"
   * including whitespace.
   */
  public static String escape(String name) {
    return name.replaceFirst(".*:([a-zA-Z#0-9]+).*", "$1").replaceAll("[^A-Za-z0-9_-]", "_");
  }

  public static PCollection.IsBounded isBounded(RunnerApi.PCollection pCollection) {
    return pCollection.getIsBounded() == RunnerApi.IsBounded.Enum.BOUNDED
        ? PCollection.IsBounded.BOUNDED
        : PCollection.IsBounded.UNBOUNDED;
  }
}
