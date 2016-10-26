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

package org.apache.beam.runners.spark;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;

/**
 * Interface for retrieving the result(s) of running a pipeline. Allows us to translate between
 * {@code PObject<T>}s or {@code PCollection<T>}s and Ts or collections of Ts.
 */
public interface EvaluationResult extends PipelineResult {
  /**
   * Retrieves an iterable of results associated with the PCollection passed in.
   *
   * @param pcollection Collection we wish to translate.
   * @param <T>         Type of elements contained in collection.
   * @return Natively types result associated with collection.
   */
  <T> Iterable<T> get(PCollection<T> pcollection);

  /**
   * Retrieve an object of Type T associated with the PValue passed in.
   *
   * @param pval PValue to retrieve associated data for.
   * @param <T>  Type of object to return.
   * @return Native object.
   */
  <T> T get(PValue pval);

  /**
   * Retrieves the final value of the aggregator.
   *
   * @param aggName    name of aggregator.
   * @param resultType Class of final result of aggregation.
   * @param <T>        Type of final result of aggregation.
   * @return Result of aggregation associated with specified name.
   */
  <T> T getAggregatorValue(String aggName, Class<T> resultType);

  /**
   * Releases any runtime resources, including distributed-execution contexts currently held by
   * this EvaluationResult; once close() has been called,
   * {@link EvaluationResult#get(PCollection)} might
   * not work for subsequent calls.
   *
   * @param gracefully true if Spark should finish all ongoing work before closing.
   */
  void close(boolean gracefully);
}
