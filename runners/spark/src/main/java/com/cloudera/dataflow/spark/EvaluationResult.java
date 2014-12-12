/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.dataflow.spark;

import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PObject;

/**
 * Interfacing for retrieve the result(s) of running a pipeline. Allows us to translate between
 * PObjects<T> or PCollections<T> and T's or coolections of T's.
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
     * Retrieve an object of Type T associated with the PObject passed in.
     *
     * @param pobject PObject we wish to translate.
     * @param <T>     Type of object to return.
     * @return Native object.
     */
    <T> T get(PObject<T> pobject);

    /**
     * Retrieves the final value of the aggregator.
     *
     * @param aggName    name of aggragtor.
     * @param resultType Class of final result of aggregatiohn.
     * @param <T>        Type of final result of aggregation.
     * @return Result of aggregation associated with specified name.
     */
    <T> T getAggregatorValue(String aggName, Class<T> resultType);
}
