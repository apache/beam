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

import com.google.cloud.dataflow.contrib.firebase.io.FirebaseSource.FirebaseReader;
import com.google.cloud.dataflow.sdk.io.UnboundedSource.CheckpointMark;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.joda.time.Instant;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import javax.annotation.Nullable;

/**
 * Maintains a {@link PriorityBlockingQueue} to store the {@link Record}s created by
 * {@link FirebaseReader}, on error these records are replayed on the new Reader to prevent
 * dataloss.
 * @param <T>
 */
public class FirebaseCheckpoint<T> implements CheckpointMark {

  //Oldest on top
  private PriorityBlockingQueue<Record<T>> unread;
  private LinkedBlockingQueue<Record<T>> read;

  private Record<T> cur;

  @JsonCreator
  public FirebaseCheckpoint(
      @JsonProperty("read") Record<T>[] read,
      @JsonProperty("unread") Record<T>[] unread,
      @JsonProperty("cur") Record<T> cur){
    this.read = new LinkedBlockingQueue<>(read.length);
    this.read.addAll(Arrays.asList(read));
    this.unread = new PriorityBlockingQueue<>(unread.length);
    this.unread.addAll(Arrays.asList(unread));
    this.cur = cur;
  }

  public FirebaseCheckpoint(){
    this.unread = new PriorityBlockingQueue<>();
    this.read = new LinkedBlockingQueue<>();
  }

  @Override
  public void finalizeCheckpoint() throws IOException {
    this.read = new LinkedBlockingQueue<>();
  }

  public Instant minTimestamp(){
    @Nullable Record<T> next = unread.peek();
    // NOTE: records compare using their timestamps
    if (next == null || cur.compareTo(next) < 0) {
      return cur.timestamp;
    } else {
      return next.timestamp;
    }
  }

  public FirebaseCheckpoint<T> reset(){
    unread.addAll(read);
    try {
      this.finalizeCheckpoint();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return this;
  }

  public void put(Record<T> e){
    unread.put(e);
  }

  public Record<T> read(){
    return this.cur;
  }

  public boolean advance(){
    if (cur != null){
      read.add(cur);
    }
    cur = unread.poll();
    return cur != null;
  }

  /**
   * For {@link FirebaseCheckpointCoder}.
   */
  @SuppressWarnings("unchecked")
  public Record<T>[] getRead(){
    return this.read.toArray(new Record[read.size()]);
  }

  /**
   * For {@link FirebaseCheckpointCoder}.
   */
  @SuppressWarnings("unchecked")
  public Record<T>[] getUnread(){
    return this.unread.toArray(new Record[unread.size()]);
  }

  public Record<T> getCur(){
    return this.cur;
  }


}
