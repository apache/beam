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
package org.apache.beam.runners.spark.structuredstreaming.translation;

import org.apache.beam.runners.spark.structuredstreaming.translation.batch.PipelineTranslatorBatch;
import org.apache.beam.sdk.annotations.Internal;

/**
 * Factory to create the {@link PipelineTranslator} matching the execution mode of the pipeline.
 *
 * <p>This file shadows the shared base version of the same name found under {@code
 * runners/spark/src}. The Spark 4 module compiles a merged source tree of the shared base plus
 * {@code runners/spark/4/src}, with a later-wins duplicate strategy, so this copy silently replaces
 * the base one for the Spark 4 module only. The base version keeps throwing for streaming, this one
 * dispatches to the real streaming translator.
 */
@Internal
public final class PipelineTranslatorFactory {
  private PipelineTranslatorFactory() {}

  /** Creates a {@link PipelineTranslator} for the given execution mode. */
  public static PipelineTranslator create(boolean streaming) {
    return streaming ? new PipelineTranslatorStreaming() : new PipelineTranslatorBatch();
  }
}
