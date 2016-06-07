/**
 * Copyright (c) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not  use this file except  in compliance with the License. You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.contrib.firebase.io;

import com.google.cloud.dataflow.contrib.firebase.events.ChildRemoved;
import com.google.cloud.dataflow.sdk.transforms.DoFnTester;
import com.google.cloud.dataflow.sdk.values.KV;

import com.firebase.client.Firebase;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Triggers {@link ChildRemoved} events by adding a large number of data and removing it.
 */
public class ChildRemovedTest extends FirebaseChildTest {

  private KV<String, Object>[] someData;
  private KV<String, Object>[] nulls;

  @SuppressWarnings("unchecked")
  @Override
  public void prepareData(List<Map<String, Object>> testData) {
    Set<Entry<String, Object>> entrySet = testData.get(0).entrySet();

    nulls = new KV[entrySet.size()];
    someData = new KV[entrySet.size()];
    int i = 0;
    for (Entry<String, Object> entry : entrySet){
      LOGGER.info(entry.getKey());
      nulls[i] = KV.of(entry.getKey(), null);
      someData[i] = KV.of(entry.getKey(), entry.getValue());
      i++;
    }
  }

  @Override
  public void triggerEvents(Firebase f) {
    DoFnTester<KV<String, Object>, Void> setter = DoFnTester.of(
        new DoFirebaseSet(f.toString(), auther));
    setter.processBatch(someData);
    setter.processBatch(nulls);
  }
}
