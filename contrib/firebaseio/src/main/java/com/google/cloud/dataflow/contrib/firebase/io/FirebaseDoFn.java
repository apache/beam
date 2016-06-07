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
import com.firebase.client.Firebase.CompletionListener;
import com.firebase.client.FirebaseError;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Abstract superclass for DoFns that need a {@link Firebase} ref.
 * This class allows concurrent writes to a Firebase within a bundle and blocks once all writes in
 * the bundle are complete, by sharing a single lock across the bundle
 * (via the {@code FirebaseListener} class)
 */
public abstract class FirebaseDoFn<InputT, OutputT> extends DoFn<InputT, OutputT> {

  private static final long serialVersionUID = 2668706582381990870L;

  final String url;
  final FirebaseAuthenticator auther;

  transient Firebase root;
  private transient Semaphore lock;

  /**
   * Releases a permit on the {@link Semaphore} when
   * {@link CompletionListener#onComplete(FirebaseError, Firebase)} is called.
   */
  protected class FirebaseListener implements CompletionListener{
    @Override
    public void onComplete(FirebaseError err, Firebase arg1) {
      lock.release();
      if (err != null){
        throw err.toException();
      }
    }
  }

  /**
   * @param url Fully qualified address to the Firebase ref which this DoFn uses.
   * @param auther Used to authenticate to Firebase
   */
  public FirebaseDoFn(String url, FirebaseAuthenticator auther){
    this.url = url;
    this.auther = auther;
  }

  @Override
  public void startBundle(Context c) throws Exception {
    root = new Firebase(this.url);
    auther.authenticate(root);
    //No need to throttle, that will be taken care of by bundle size
    lock = new Semaphore(Integer.MAX_VALUE);
  }

  /**
   * @param context ProcessContext from the Pipeline
   * @param listener Used to indicate when the asynchronous operation is complete
   */
  public abstract void asyncProcessElement(
      DoFn<InputT, OutputT>.ProcessContext context,
      FirebaseListener listener);

  @Override
  public void processElement(DoFn<InputT, OutputT>.ProcessContext context)
      throws InterruptedException{
    lock.acquire();
    this.asyncProcessElement(context, new FirebaseListener());
  }

  @Override
  public void finishBundle(Context c) throws InterruptedException{
    //Wait "forever"
    lock.tryAcquire(Integer.MAX_VALUE, Long.MAX_VALUE, TimeUnit.DAYS);
  }
}
