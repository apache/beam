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
package org.apache.beam.sdk.extensions.euphoria.core.translate;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * PViews store created PCollectionView from PCollection. It's useful when translator is creating
 * PCollectionView and later from another operator the same PCollectionView is created
 * from the same PCollection.
 * To prevent creating same PCollectionViews multiple times, use this class.
 */
public class PViews {

  private static Map<PCollection<?>, PCollectionView<?>> pViewsMap;

  public static Map<PCollection<?>, PCollectionView<?>> getMap() {
    if (pViewsMap == null) {
      pViewsMap = new HashMap<>();
    }
    return pViewsMap;
  }

  @SuppressWarnings("unchecked")
  public static <K, V> PCollectionView<Map<K, Iterable<V>>> createMultimapIfAbsent(
      PCollection<?> pCollectionKey, final PCollection<KV<K, V>> pCollectionToView) {
    return (PCollectionView<Map<K, Iterable<V>>>)
        getMap().computeIfAbsent(pCollectionKey, (i) -> pCollectionToView.apply(View.asMultimap()));
  }
}
