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

import com.google.cloud.dataflow.contrib.firebase.events.FirebaseEvent;
import com.google.cloud.dataflow.contrib.firebase.events.ValueChanged;
import com.google.cloud.dataflow.contrib.firebase.utils.FirebaseAuthenticator;
import com.google.cloud.dataflow.sdk.transforms.DoFnTester;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebase.client.DataSnapshot;
import com.firebase.client.Firebase;
import com.firebase.client.FirebaseError;
import com.firebase.client.ValueEventListener;

import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Collection;
import java.util.List;
import java.util.Map;



/**
 * Triggers value changes.
 * Potentially flakey since it relies on spacing out value changes to avoid events being
 * globbed by Firebase Client Library
 */
@RunWith(JUnit4.class)
public class FirebaseValueTest extends BaseFirebaseSourceTest {

  ValueEventListener listener;
  List<Map<String, Object>> someData;

  @SuppressWarnings("unchecked")
  @Override
  public void prepareData(List<Map<String, Object>> testData) {
    someData = testData;
    try {
      LOGGER.info(new ObjectMapper().writeValueAsString(someData));
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void triggerEvents(Firebase f) {
    DoFnTester<Map<String, Object>, Void> updater = DoFnTester.of(
        new DoFirebaseUpdate(f.toString(), auther));
    for (Map<String, Object> data : someData){
      updater.processBatch(data);
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void addListener(Firebase f, final Collection<FirebaseEvent<JsonNode>> events) {
    listener = f.addValueEventListener(new ValueEventListener(){

      @Override
      public void onCancelled(FirebaseError err) { }

      @Override
      public void onDataChange(DataSnapshot data) {
        events.add(new ValueChanged<JsonNode>(data));
      }
    });
  }

  @Override
  public void removeListener(Firebase f) {
    f.removeEventListener(listener);
  }

  @Override
  public FirebaseSource<JsonNode> makeSource(
      FirebaseAuthenticator auther,
      Firebase f) {
    return FirebaseSource.valueListenerOf(JsonNode.class, auther, f);
  }

}
