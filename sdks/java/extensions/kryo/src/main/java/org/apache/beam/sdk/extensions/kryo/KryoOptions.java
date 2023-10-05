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
package org.apache.beam.sdk.extensions.kryo;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * {@link PipelineOptions} which allows passing {@link com.esotericsoftware.kryo.Kryo} parameters.
 */
@Description("Options for KryoCoder")
public interface KryoOptions extends PipelineOptions {

  @JsonIgnore
  @Description("Set buffer size")
  @Default.Integer(64 * 1024)
  int getKryoBufferSize();

  void setKryoBufferSize(int bufferSize);

  @JsonIgnore
  @Description("Set to false to disable reference tracking")
  @Default.Boolean(true)
  boolean getKryoReferences();

  void setKryoReferences(boolean references);

  @JsonIgnore
  @Description("Set to true to enable required registration")
  @Default.Boolean(false)
  boolean getKryoRegistrationRequired();

  void setKryoRegistrationRequired(boolean registrationRequired);
}
