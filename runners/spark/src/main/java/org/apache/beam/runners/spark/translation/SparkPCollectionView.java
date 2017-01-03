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

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.util.BroadcastHelper;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * SparkPCollectionView is used to pass serialized views to lambdas.
 */
public class SparkPCollectionView implements Serializable {

    // Holds the view --> broadcast mapping. Transient so it will be null from resume
    private transient volatile Map<PCollectionView<?>, BroadcastHelper> broadcastHelperMap = null;

    // Holds the Actual data of the views in serialize form
    private Map<PCollectionView<?>,
        Tuple2<byte[], Coder<Iterable<WindowedValue<?>>>>> pviews =
            new LinkedHashMap<>();

    // Driver only - during evaluation stage
    void putPView(
        PCollectionView<?> view,
        Iterable<WindowedValue<?>> value,
        Coder<Iterable<WindowedValue<?>>> coder,
        JavaSparkContext context) {

        pviews.put(view, new Tuple2<>(CoderHelpers.toByteArray(value, coder), coder));
        // overwrite/create broadcast
        getPCollectionView(view, context, true);
    }

    BroadcastHelper getPCollectionView(
        PCollectionView<?> view,
        JavaSparkContext context) {
        return getPCollectionView(view, context, false);
    }

    private BroadcastHelper getPCollectionView(
        PCollectionView<?> view,
        JavaSparkContext context,
        boolean overwrite) {
        // initialize broadcastHelperMap if needed
        if (broadcastHelperMap == null) {
            synchronized (SparkPCollectionView.class) {
                if (broadcastHelperMap == null) {
                    broadcastHelperMap = new LinkedHashMap<>();
                }
            }
        }

        //lazily broadcast views
        BroadcastHelper helper = broadcastHelperMap.get(view);
        if (helper == null) {
            synchronized (SparkPCollectionView.class) {
                helper = broadcastHelperMap.get(view);
                if (helper == null) {
                    helper = createBroadcastHelper(view, context);
                }
            }
        } else if (overwrite) {
            synchronized (SparkPCollectionView.class) {
                // Currently unsynchronized unpersist, if needed can be changed to blocking
                helper.unpersist();
                helper = createBroadcastHelper(view, context);
            }
        }
        return helper;
    }

    private BroadcastHelper createBroadcastHelper(
        PCollectionView<?> view,
        JavaSparkContext context) {
        Tuple2<byte[], Coder<Iterable<WindowedValue<?>>>> tuple2 = pviews.get(view);
        BroadcastHelper helper = BroadcastHelper.create(tuple2._1, tuple2._2);
        helper.broadcast(context);
        broadcastHelperMap.put(view, helper);
        return helper;
    }
}
