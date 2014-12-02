/**
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
package com.cloudera.dataflow.spark;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.PObject;
import com.google.cloud.dataflow.sdk.values.POutput;
import com.google.cloud.dataflow.sdk.values.PValue;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.Map;
import java.util.Set;

public class EvaluationContext implements SparkPipelineRunner.EvaluationResult {
  final JavaSparkContext jsc;
  final Pipeline pipeline;
  final Map<PValue, JavaRDDLike> rdds = Maps.newHashMap();
  final Set<PValue> multireads = Sets.newHashSet();
  final Map<PObject, Object> localPObjects = Maps.newHashMap();

  public EvaluationContext(String master, Pipeline pipeline) {
    this.jsc = new JavaSparkContext(master, "dataflow");
    this.pipeline = pipeline;
  }

  JavaSparkContext getSparkContext() {
    return jsc;
  }
  Pipeline getPipeline() { return pipeline; }

  POutput getOutput(PTransform transform) {
    return pipeline.getOutput(transform);
  }

  void setOutputRDD(PTransform transform, JavaRDDLike rdd) {
    rdds.put((PValue) getOutput(transform), rdd);
  }

  void setPObjectValue(PObject pobj, Object value) {
    localPObjects.put(pobj, value);
  }

  JavaRDDLike getRDD(PValue pvalue) {
    JavaRDDLike rdd = rdds.get(pvalue);
    if (multireads.contains(pvalue)) {
      // Ensure the RDD is marked as cached
      rdd.rdd().cache();
    } else {
      multireads.add(pvalue);
    }
    return rdd;
  }

  JavaRDDLike getInputRDD(PTransform transform) {
    return getRDD((PValue) pipeline.getInput(transform));
  }

  <T> BroadcastHelper<T> getBroadcastHelper(PObject<T> value) {
    Coder<T> coder = value.getCoder();
    Broadcast<byte[]> bcast = jsc.broadcast(CoderHelpers.toByteArray(resolve(value), coder));
    return new BroadcastHelper<>(bcast, coder);
  }

  <T> T resolve(PObject<T> value) {
    if (localPObjects.containsKey(value)) {
      return (T) localPObjects.get(value);
    } else if (rdds.containsKey(value)) {
      JavaRDDLike rdd = rdds.get(value);
      //TODO: probably some work to do here
      T res = (T) Iterables.getOnlyElement(rdd.collect());
      localPObjects.put(value, res);
      return res;
    }
    throw new IllegalStateException("Cannot resolve un-known PObject: " + value);
  }
}
