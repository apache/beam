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
package org.apache.beam.sdk.ml.remoteinference.base;

import java.util.List;

/**
 * Interface for model-specific handlers that perform remote inference operations.
 *
 * <p>Implementations of this interface encapsulate all logic for communicating with a
 * specific remote inference service. Each handler is responsible for:
 * <ul>
 *   <li>Initializing and managing client connections</li>
 *   <li>Converting Beam inputs to service-specific request formats</li>
 *   <li>Making inference API calls</li>
 *   <li>Converting service responses to Beam output types</li>
 *   <li>Handling errors and retries if applicable</li>
 * </ul>
 *
 * <h3>Lifecycle</h3>
 *
 * <p>Handler instances follow this lifecycle:
 * <ol>
 *   <li>Instantiation via no-argument constructor</li>
 *   <li>{@link #createClient} called with parameters during setup</li>
 *   <li>{@link #request} called for each batch of inputs</li>
 * </ol>
 *
 *
 * <p>Handlers typically contain non-serializable client objects.
 * Mark client fields as {@code transient} and initialize them in {@link #createClient}
 *
 * <h3>Batching Considerations</h3>
 *
 * <p>The {@link #request} method receives a list of inputs. Implementations should:
 * <ul>
 *   <li>Batch inputs efficiently if the service supports batch inference</li>
 *   <li>Return results in the same order as inputs</li>
 *   <li>Maintain input-output correspondence in {@link PredictionResult}</li>
 * </ul>
 *
 */
public interface BaseModelHandler<ParamT extends BaseModelParameters, InputT extends BaseInput, OutputT extends BaseResponse> {
  /**
   *  Initializes the remote model client with the provided parameters.
   */
  public void createClient(ParamT parameters);

 /**
 * Performs inference on a batch of inputs and returns the results.
 */
  public Iterable<PredictionResult<InputT, OutputT>> request(List<InputT> input);

}
