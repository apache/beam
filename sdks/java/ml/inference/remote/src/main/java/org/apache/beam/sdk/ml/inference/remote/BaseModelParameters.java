/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.ml.inference.remote;

import java.io.Serializable;

/**
 * Base interface for defining model-specific parameters used to configure remote inference clients.
 *
 * <p>Implementations of this interface encapsulate all configuration needed to initialize
 * and communicate with a remote model inference service. This typically includes:
 * <ul>
 *   <li>Authentication credentials (API keys, tokens)</li>
 *   <li>Model identifiers or names</li>
 *   <li>Endpoint URLs or connection settings</li>
 *   <li>Inference configuration (temperature, max tokens, timeout values, etc.)</li>
 * </ul>
 *
 * <p>Parameters must be serializable. Consider using
 * the builder pattern for complex parameter objects.
 *
 */
public interface BaseModelParameters extends Serializable {

}
