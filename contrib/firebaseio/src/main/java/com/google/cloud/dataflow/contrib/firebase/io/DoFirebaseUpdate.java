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

import com.firebase.client.Firebase;

import java.util.Map;

/**
 * Updates the element at the specified url, using {@link Firebase#updateChildren(Map)}.
 */
public class DoFirebaseUpdate extends FirebaseDoFn<Map<String, Object>, Void> {

  private static final long serialVersionUID = 937268867072092459L;


  public DoFirebaseUpdate(String url, FirebaseAuthenticator auther) {
    super(url, auther);
  }

  @Override
  public void asyncProcessElement(DoFn<Map<String, Object>, Void>.ProcessContext context,
      FirebaseDoFn<Map<String, Object>, Void>.FirebaseListener listener) {
    root.updateChildren(context.element(), listener);
  }

}


