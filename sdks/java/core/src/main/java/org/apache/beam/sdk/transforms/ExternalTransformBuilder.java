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
package org.apache.beam.sdk.transforms;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

/**
 * An interface for building a transform from an externally provided configuration.
 *
 * <p>Classes which implement this interface will be instantiated externally and require a zero-args
 * constructor. The {@code buildExternal} method will be called with the configuration object as a
 * parameter.
 *
 * <p>This builder needs to be registered alongside with a URN through {@link
 * ExternalTransformRegistrar}. Note that the configuration requires setters for all configuration
 * parameters, e.g. if there is a parameter "start", there should be a corresponding setter
 * "setStart".
 *
 * @param <ConfigT> A configuration object which will be populated with the external configuration.
 * @param <InputT> The input type of the externally configured PTransform.
 * @param <OutputT> The output type of the externally configured PTransform.
 */
@Experimental(Kind.PORTABILITY)
public interface ExternalTransformBuilder<ConfigT, InputT extends PInput, OutputT extends POutput> {

  /** Builds the transform after it has been configured. */
  PTransform<InputT, OutputT> buildExternal(ConfigT configuration);
}
