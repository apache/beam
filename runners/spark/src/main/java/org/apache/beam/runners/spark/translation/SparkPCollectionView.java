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
import org.apache.beam.runners.spark.util.SideInputBroadcast;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** SparkPCollectionView is used to pass serialized views to lambdas. */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class SparkPCollectionView implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(SparkPCollectionView.class);
  // Holds the view --> broadcast mapping. Transient so it will be null from resume
  private transient volatile Map<PCollectionView<?>, SideInputBroadcast> broadcastHelperMap = null;

  /** Type of side input. */
  public enum Type {
    /** for fixed inputs. */
    STATIC,
    /** for dynamically updated inputs. */
    STREAMING
  }

  // Holds the Actual data of the views in serialize form
  private final Map<PCollectionView<?>, SideInputMetadata> pviews = new LinkedHashMap<>();

  public void putPView(
      PCollectionView<?> view,
      Iterable<WindowedValue<?>> value,
      Coder<Iterable<WindowedValue<?>>> coder) {
    this.putPView(view, value, coder, Type.STATIC);
  }

  public void putStreamingPView(
      PCollectionView<?> view,
      Iterable<WindowedValue<?>> value,
      Coder<Iterable<WindowedValue<?>>> coder) {
    this.putPView(view, value, coder, Type.STREAMING);
  }

  // Driver only - during evaluation stage
  private void putPView(
      PCollectionView<?> view,
      Iterable<WindowedValue<?>> value,
      Coder<Iterable<WindowedValue<?>>> coder,
      Type type) {

    pviews.put(view, SideInputMetadata.create(CoderHelpers.toByteArray(value, coder), type, coder));

    // Currently unsynchronized unpersist, if needed can be changed to blocking
    if (broadcastHelperMap != null) {
      synchronized (SparkPCollectionView.class) {
        SideInputBroadcast helper = broadcastHelperMap.get(view);
        if (helper != null) {
          helper.unpersist();
          broadcastHelperMap.remove(view);
        }
      }
    }
  }

  SideInputBroadcast getPCollectionView(PCollectionView<?> view, JavaSparkContext context) {
    // initialize broadcastHelperMap if needed
    if (broadcastHelperMap == null) {
      synchronized (SparkPCollectionView.class) {
        if (broadcastHelperMap == null) {
          broadcastHelperMap = new LinkedHashMap<>();
        }
      }
    }

    // lazily broadcast views
    SideInputBroadcast helper = broadcastHelperMap.get(view);
    if (helper == null) {
      synchronized (SparkPCollectionView.class) {
        helper = broadcastHelperMap.get(view);
        if (helper == null) {
          helper = createBroadcastHelper(view, context);
        }
      }
    }
    return helper;
  }

  private SideInputBroadcast createBroadcastHelper(
      PCollectionView<?> view, JavaSparkContext context) {
    final SideInputMetadata sideInputMetadata = pviews.get(view);
    SideInputBroadcast helper = sideInputMetadata.toSideInputBroadcast();
    String pCollectionName =
        view.getPCollection() != null ? view.getPCollection().getName() : "UNKNOWN";
    LOG.debug(
        "Broadcasting [size={}B] view {} from pCollection {}",
        helper.getBroadcastSizeEstimate(),
        view,
        pCollectionName);
    helper.broadcast(context);
    broadcastHelperMap.put(view, helper);
    return helper;
  }
}
