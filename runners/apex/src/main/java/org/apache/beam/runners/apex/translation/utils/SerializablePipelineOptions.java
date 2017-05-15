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
package org.apache.beam.runners.apex.translation.utils;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.runners.apex.ApexPipelineOptions;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.common.ReflectHelpers;

/**
 * A wrapper to enable serialization of {@link PipelineOptions}.
 */
public class SerializablePipelineOptions implements Externalizable {

  /* Used to ensure we initialize file systems exactly once, because it's a slow operation. */
  private static final AtomicBoolean FILE_SYSTEMS_INTIIALIZED = new AtomicBoolean(false);

  private transient ApexPipelineOptions pipelineOptions;

  public SerializablePipelineOptions(ApexPipelineOptions pipelineOptions) {
    this.pipelineOptions = pipelineOptions;
  }

  public SerializablePipelineOptions() {
  }

  public ApexPipelineOptions get() {
    return this.pipelineOptions;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeUTF(createMapper().writeValueAsString(pipelineOptions));
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    String s = in.readUTF();
    this.pipelineOptions = createMapper().readValue(s, PipelineOptions.class)
        .as(ApexPipelineOptions.class);

    if (FILE_SYSTEMS_INTIIALIZED.compareAndSet(false, true)) {
      FileSystems.setDefaultPipelineOptions(pipelineOptions);
    }
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
}
