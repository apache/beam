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
 * Sets a value using {@link Firebase#setValue(Object)} where the {@code String} key is the
 * path to the child to set the value on. Set a path as {@code null} to remove a value.
 */
public class DoFirebaseSet extends FirebaseDoFn<KV<String, Object>, Void> {

  private static final long serialVersionUID = -4587959210797506754L;

  public DoFirebaseSet(String url, FirebaseAuthenticator auther) {
    super(url, auther);
  }

  @Override
  public void asyncProcessElement(DoFn<KV<String, Object>, Void>.ProcessContext context,
      FirebaseDoFn<KV<String, Object>, Void>.FirebaseListener listener) {
    root.child(context.element().getKey())
    .setValue(context.element().getValue(), listener);
  }
}


