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

import com.google.cloud.dataflow.contrib.firebase.utils.FirebaseAuthenticator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;

import com.firebase.client.Firebase;

/**
 * Uses {@link Firebase#setValue(Object, Object)} to set an object with a priority.
 * This is a bad pattern, and only used to trigger ChildMovedTest. Users should prefer
 * {@link Firebase#orderByKey()} to {@link Firebase#orderByPriority()} and as such have no need
 * to set priority.
 */
@Deprecated
class DoFirebaseSetWithPriority extends
        FirebaseDoFn<KV<String, KV<Object, Object>>, Void> {

  private static final long serialVersionUID = 1321447767620652932L;

  DoFirebaseSetWithPriority(String url, FirebaseAuthenticator auther) {
    super(url, auther);
  }

  @Override
  public void asyncProcessElement(
      DoFn<KV<String, KV<Object, Object>>, Void>.ProcessContext context,
      FirebaseDoFn<KV<String, KV<Object, Object>>, Void>.FirebaseListener listener) {
    root.child(context.element().getKey())
    .setValue(
        context.element().getValue().getKey(),
        context.element().getValue().getValue(),
        listener);
  }
}

