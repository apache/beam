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

package org.apache.beam.runners.spark.translation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.Serializable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.common.ReflectHelpers;

/**
 * The SparkRuntimeContext allows us to define useful features on the client side before our
 * data flow program is launched.
 */
public class SparkRuntimeContext implements Serializable {
  private final String serializedPipelineOptions;
  private transient CoderRegistry coderRegistry;

  SparkRuntimeContext(Pipeline pipeline, PipelineOptions options) {
    this.serializedPipelineOptions = serializePipelineOptions(options);
  }

  /**
   * Use an {@link ObjectMapper} configured with any {@link Module}s in the class path allowing
   * for user specified configuration injection into the ObjectMapper. This supports user custom
   * types on {@link PipelineOptions}.
   */
  private static ObjectMapper createMapper() {
    return new ObjectMapper().registerModules(
        ObjectMapper.findModules(ReflectHelpers.findClassLoader()));
  }

  private String serializePipelineOptions(PipelineOptions pipelineOptions) {
    try {
      return createMapper().writeValueAsString(pipelineOptions);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize the pipeline options.", e);
    }
  }

  private static PipelineOptions deserializePipelineOptions(String serializedPipelineOptions) {
    try {
      return createMapper().readValue(serializedPipelineOptions, PipelineOptions.class);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to deserialize the pipeline options.", e);
    }
  }

  public PipelineOptions getPipelineOptions() {
    return PipelineOptionsHolder.getOrInit(serializedPipelineOptions);
  }

  public CoderRegistry getCoderRegistry() {
    if (coderRegistry == null) {
      coderRegistry = CoderRegistry.createDefault();
    }
    return coderRegistry;
  }

  private static class PipelineOptionsHolder {
    // on executors, this should deserialize once.
    private static transient volatile PipelineOptions pipelineOptions = null;

    static PipelineOptions getOrInit(String serializedPipelineOptions) {
      if (pipelineOptions == null) {
        synchronized (PipelineOptionsHolder.class) {
          if (pipelineOptions == null) {
            pipelineOptions = deserializePipelineOptions(serializedPipelineOptions);
          }
        }
        // Register standard FileSystems.
        FileSystems.setDefaultPipelineOptions(pipelineOptions);
      }
      return pipelineOptions;
    }
  }
}
