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
package com.google.cloud.dataflow.contrib.firebase.utils;

import com.firebase.client.AuthData;
import com.firebase.client.Firebase;
import com.firebase.client.Firebase.AuthResultHandler;
import com.firebase.client.FirebaseError;
import com.firebase.client.FirebaseException;

import java.util.concurrent.CountDownLatch;

/**
 * Wrapper for {@link Firebase#authWithPassword(String, String, AuthResultHandler)}.
 */
public class FirebasePasswordAuthenticator extends FirebaseAuthenticator {

  private static final long serialVersionUID = -3274906800271012577L;
  private final String email;
  private final String password;
  private transient AuthData result;

  public FirebasePasswordAuthenticator(String email, String password){
    this.email = email;
    this.password = password;
  }

  @Override
  public AuthData authenticate(Firebase firebase) throws FirebaseException, InterruptedException {
    final CountDownLatch done = new CountDownLatch(1);
    firebase.authWithPassword(this.email, this.password, new AuthResultHandler() {
      @Override
      public void onAuthenticationError(FirebaseError err) {
        done.countDown();
        throw err.toException();
      }
      @Override
      public void onAuthenticated(AuthData arg0) {
        result = arg0;
        done.countDown();
      }
    });
    done.await();
    return result;
  }
}
