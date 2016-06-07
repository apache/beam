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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping;
import com.fasterxml.jackson.databind.SerializationFeature;

import com.firebase.client.DataSnapshot;
import com.firebase.client.GenericTypeIndicator;

import java.util.Objects;

/**
 * Supertype for all {@link FirebaseEvent}s.
 * @param <T> type that {@link DataSnapshot} is parsed as.
**/
public class FirebaseEvent<T>{

  private static ObjectMapper mapper = new ObjectMapper()
      .enableDefaultTyping(DefaultTyping.NON_FINAL)
      .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

  public final String key;
  public final T data;

  protected FirebaseEvent(String key, T data){
    this.key = key;
    this.data = data;
  }

  protected FirebaseEvent(DataSnapshot snapshot){
    this(snapshot.getKey(), snapshot.getValue(new GenericTypeIndicator<T>(){}));
  }

  @Override
  public int hashCode(){
    return Objects.hash(key, data);
  }

  /**
   * Does a deep equality check by delegating to {@code T.equals()}.
   */
  @Override
  public boolean equals(Object e){
    if (this.getClass().isInstance(e)){
      FirebaseEvent<?> other = (FirebaseEvent<?>) e;
      return Objects.equals(this.key, other.key) && Objects.equals(this.data, other.data);
    }
    return false;
  }

  @Override
  public String toString(){
    try {
      return mapper.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      return e.toString();
    }
  }
}
