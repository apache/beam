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
import com.google.cloud.dataflow.contrib.firebase.events.ChildChanged;
import com.google.cloud.dataflow.contrib.firebase.events.ChildMoved;
import com.google.cloud.dataflow.contrib.firebase.events.ChildRemoved;
import com.google.cloud.dataflow.contrib.firebase.events.FirebaseEvent;
import com.google.cloud.dataflow.contrib.firebase.utils.FirebaseAuthenticator;

import com.fasterxml.jackson.databind.JsonNode;
import com.firebase.client.ChildEventListener;
import com.firebase.client.DataSnapshot;
import com.firebase.client.Firebase;
import com.firebase.client.FirebaseError;

import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Collection;

/**
 * Parent test for all tests which trigger events in a {@link ChildEventListener}.
 **/
@RunWith(JUnit4.class)
public abstract class FirebaseChildTest extends BaseFirebaseSourceTest {

  ChildEventListener listener;

  @Override
  public void addListener(Firebase f, final Collection<FirebaseEvent<JsonNode>> events) {
    listener = f.addChildEventListener(new ChildEventListener(){

      @Override
      public void onCancelled(FirebaseError err) {  }

      @Override
      public void onChildAdded(DataSnapshot data, String previousChildName) {
        events.add(new ChildAdded<JsonNode>(data, previousChildName));
      }

      @Override
      public void onChildChanged(DataSnapshot data, String previousChildName) {
        events.add(new ChildChanged<JsonNode>(data, previousChildName));
      }

      @Override
      public void onChildMoved(DataSnapshot data, String previousChildName) {
        events.add(new ChildMoved<JsonNode>(data, previousChildName));
      }

      @Override
      public void onChildRemoved(DataSnapshot data) {
        events.add(new ChildRemoved<JsonNode>(data));
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
    return FirebaseSource.childListenerOf(JsonNode.class, auther, f);

  }

}
