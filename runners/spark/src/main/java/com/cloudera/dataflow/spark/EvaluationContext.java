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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.PObject;
import com.google.cloud.dataflow.sdk.values.PObjectTuple;
import com.google.cloud.dataflow.sdk.values.PObjectValueTuple;
import com.google.cloud.dataflow.sdk.values.POutput;
import com.google.cloud.dataflow.sdk.values.PValue;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

/**
 * Evaluation context allows us to define how pipeline instructions
 */
public class EvaluationContext implements EvaluationResult {
    private final JavaSparkContext jsc;
    private final Pipeline pipeline;
    private final SparkRuntimeContext runtime;
    private final Map<PValue, JavaRDDLike<?,?>> rdds = new HashMap<>();
    private final Set<PValue> multireads = new HashSet<>();
    private final Map<PObject<?>, Object> pobjects = new HashMap<>();

    public EvaluationContext(JavaSparkContext jsc, Pipeline pipeline) {
        this.jsc = jsc;
        this.pipeline = pipeline;
        this.runtime = new SparkRuntimeContext(jsc, pipeline);
    }

    JavaSparkContext getSparkContext() {
        return jsc;
    }

    Pipeline getPipeline() {
        return pipeline;
    }

    SparkRuntimeContext getRuntimeContext() {
        return runtime;
    }

    <I extends PInput> I getInput(PTransform<I, ?> transform) {
        @SuppressWarnings("unchecked")
        I input = (I) pipeline.getInput(transform);
        return input;
    }

    <O extends POutput> O getOutput(PTransform<?, O> transform) {
        @SuppressWarnings("unchecked")
        O output = (O) pipeline.getOutput(transform);
        return output;
    }

    void setOutputRDD(PTransform<?,?> transform, JavaRDDLike<?,?> rdd) {
        rdds.put((PValue) getOutput(transform), rdd);
    }

    void setPObjectValue(PObject<?> pobject, Object value) {
        pobjects.put(pobject, value);
    }

    JavaRDDLike<?,?> getRDD(PValue pvalue) {
        JavaRDDLike<?,?> rdd = rdds.get(pvalue);
        if (multireads.contains(pvalue)) {
            // Ensure the RDD is marked as cached
            rdd.rdd().cache();
        } else {
            multireads.add(pvalue);
        }
        return rdd;
    }

    void setRDD(PValue pvalue, JavaRDDLike<?,?> rdd) {
        rdds.put(pvalue, rdd);
    }

    JavaRDDLike<?,?> getInputRDD(PTransform transform) {
        return getRDD((PValue) pipeline.getInput(transform));
    }

    <T> BroadcastHelper<T> getBroadcastHelper(PObject<T> value) {
        Coder<T> coder = value.getCoder();
        Broadcast<byte[]> bcast = jsc.broadcast(CoderHelpers.toByteArray(get(value), coder));
        return new BroadcastHelper<>(bcast, coder);
    }

    @Override
    public <T> T get(PObject<T> value) {
        if (pobjects.containsKey(value)) {
            @SuppressWarnings("unchecked")
            T result = (T) pobjects.get(value);
            return result;
        }
        if (rdds.containsKey(value)) {
            JavaRDDLike<?,?> rdd = rdds.get(value);
            //TODO: need same logic from get() method below here for serialization of bytes
            @SuppressWarnings("unchecked")
            T res = (T) Iterables.getOnlyElement(rdd.collect());
            pobjects.put(value, res);
            return res;
        }
        throw new IllegalStateException("Cannot resolve un-known PObject: " + value);
    }

    @Override
    public <T> T getAggregatorValue(String named, Class<T> resultType) {
        return runtime.getAggregatorValue(named, resultType);
    }

    @Override
    public <T> Iterable<T> get(PCollection<T> pcollection) {
        @SuppressWarnings("unchecked")
        JavaRDDLike<T,?> rdd = (JavaRDDLike<T,?>) getRDD(pcollection);
        final Coder<T> coder = pcollection.getCoder();
        JavaRDDLike<byte[],?> bytesRDD = rdd.map(CoderHelpers.toByteFunction(coder));
        List<byte[]> clientBytes = bytesRDD.collect();
        return Iterables.transform(clientBytes, new Function<byte[], T>() {
            @Override
            public T apply(byte[] bytes) {
                return (T) CoderHelpers.fromByteArray(bytes, coder);
            }
        });
    }

    PObjectValueTuple getPObjectTuple(PTransform<?,?> transform) {
        PObjectTuple pot = (PObjectTuple) pipeline.getInput(transform);
        PObjectValueTuple povt = PObjectValueTuple.empty();
        for (Map.Entry<TupleTag<?>, PObject<?>> e : pot.getAll().entrySet()) {
            povt = and(povt, e);
        }
        return povt;
    }

    private <T> PObjectValueTuple and(PObjectValueTuple povt, Map.Entry<TupleTag<?>, PObject<?>> e) {
        @SuppressWarnings("unchecked")
        TupleTag<T> ttKey = (TupleTag<T>) e.getKey();
        @SuppressWarnings("unchecked")
        PObject<T> potValue = (PObject<T>) e.getValue();
        return povt.and(ttKey, get(potValue));
    }

    void setPObjectTuple(PTransform<?,?> transform, PObjectValueTuple outputValues) {
        PObjectTuple pot = (PObjectTuple) pipeline.getOutput(transform);
        for (Map.Entry<TupleTag<?>, PObject<?>> e : pot.getAll().entrySet()) {
            pobjects.put(e.getValue(), outputValues.get(e.getKey()));
        }
    }
}
