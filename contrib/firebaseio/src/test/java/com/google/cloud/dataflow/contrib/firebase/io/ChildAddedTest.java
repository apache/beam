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

import com.google.cloud.dataflow.contrib.firebase.events.ChildAdded;
import com.google.cloud.dataflow.sdk.transforms.DoFnTester;
import com.google.cloud.dataflow.sdk.values.KV;

import com.firebase.client.Firebase;

import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;
import java.util.Map;




/**
 * Triggers {@link ChildAdded} events.
 */
@RunWith(JUnit4.class)
public class ChildAddedTest extends FirebaseChildTest {

  private KV<String, Object>[] usableData;

  @Override
  public void triggerEvents(Firebase f) {
    DoFnTester.of(new DoFirebaseSet(f.toString(), auther)).processBatch(usableData);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void prepareData(List<Map<String, Object>> testData) {
    Object[] entries = testData.toArray();

    List<KV<String, Object>> withKeys = DoFnTester.of(
        new DoFirebasePush(testRef.toString(),
        auther))
        .processBatch(entries);

    usableData = withKeys.toArray(new KV[withKeys.size()]);
  }
}
