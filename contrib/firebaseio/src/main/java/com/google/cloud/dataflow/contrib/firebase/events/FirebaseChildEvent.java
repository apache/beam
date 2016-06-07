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
package com.google.cloud.dataflow.contrib.firebase.events;

import com.firebase.client.DataSnapshot;

import java.util.Objects;

/**
 * TODO: Super class for events which come with a {@code previousChildName}.
 * @param <T> type that {@link DataSnapshot} is parsed as.
 */
public class FirebaseChildEvent<T> extends FirebaseEvent<T> {

  public final String previousChildName;

  protected FirebaseChildEvent(String key, T data, String previousChildName) {
    super(key, data);
    this.previousChildName = previousChildName;
  }

  protected FirebaseChildEvent(DataSnapshot snapshot, String previousChildName) {
    super(snapshot);
    this.previousChildName = previousChildName;
  }

  @Override
  public int hashCode(){
    return Objects.hash(previousChildName, super.hashCode());
  }

  @Override
  public boolean equals(Object e){
    if (this.getClass().isInstance(e)){
      FirebaseChildEvent<?> other = (FirebaseChildEvent<?>) e;
      return Objects.equals(this.previousChildName, other.previousChildName) && super.equals(e);
    }
    return false;
  }
}
