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

import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.spark.util.BroadcastHelper;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.spark.api.java.JavaSparkContext;


/**
 * For resilience, {@link org.apache.spark.broadcast.Broadcast}s are required to be wrapped
 * in a Singleton.
 * @see <a href="https://spark.apache.org/docs/1.6.2/streaming-programming-guide.html#accumulators-and-broadcast-variables">broadcast-variables</a>
 */
public class BroadcastSideInputs {

  private static volatile Map<TupleTag<?>, BroadcastHelper<?>> sideInputs = null;

  public static synchronized Map<TupleTag<?>, BroadcastHelper<?>>
  getOrCreateBroadcast(JavaSparkContext jsc, Map<TupleTag<?>, BroadcastHelper<?>> providedInputs) {
    if (sideInputs == null) {
      sideInputs = new HashMap<>(providedInputs);
    } else {
      for (Map.Entry<TupleTag<?>, BroadcastHelper<?>> en: providedInputs.entrySet()) {
        if (!sideInputs.containsKey(en.getKey())) {
          sideInputs.put(en.getKey(), en.getValue());
        }
      }
    }
    Map<TupleTag<?>, BroadcastHelper<?>> res =
        Maps.newHashMapWithExpectedSize(providedInputs.size());
    for (Map.Entry<TupleTag<?>, BroadcastHelper<?>> en: providedInputs.entrySet()) {
      BroadcastHelper<?> broadcastHelper = sideInputs.get(en.getKey());
      if (!broadcastHelper.isBroadcasted()) {
        broadcastHelper.broadcast(jsc);
      }
      res.put(en.getKey(), broadcastHelper);
    }
    return res;
  }

  /** For testing only. */
  public static void clear() {
    synchronized (BroadcastSideInputs.class) {
      for (BroadcastHelper<?> helper: sideInputs.values()) {
        helper.unregister();
      }
    }
  }
}
