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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.dataflow.contrib.firebase.events.ChildAdded;
import com.google.cloud.dataflow.contrib.firebase.events.ChildChanged;
import com.google.cloud.dataflow.contrib.firebase.events.ChildMoved;
import com.google.cloud.dataflow.contrib.firebase.events.ChildRemoved;
import com.google.cloud.dataflow.contrib.firebase.events.FirebaseEvent;
import com.google.cloud.dataflow.contrib.firebase.events.ValueChanged;

import com.google.cloud.dataflow.contrib.firebase.utils.FirebaseAuthenticator;
import com.google.cloud.dataflow.contrib.firebase.utils.FirebaseCheckpoint;
import com.google.cloud.dataflow.contrib.firebase.utils.FirebaseCheckpointCoder;
import com.google.cloud.dataflow.contrib.firebase.utils.FirebaseEventCoder;
import com.google.cloud.dataflow.contrib.firebase.utils.Record;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;

import com.fasterxml.jackson.databind.ObjectWriter;
import com.firebase.client.AuthData;
import com.firebase.client.ChildEventListener;
import com.firebase.client.DataSnapshot;
import com.firebase.client.Firebase;
import com.firebase.client.Firebase.CompletionListener;
import com.firebase.client.FirebaseError;
import com.firebase.client.FirebaseException;
import com.firebase.client.Query;
import com.firebase.client.ServerValue;
import com.firebase.client.ValueEventListener;

import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;



import javax.annotation.Nullable;




/**
 * Reads events from a {@code Query} and outputs them as {@link FirebaseEvent}s.
 * @param <T> Type of object to deserialize {@link DataSnapshot}s into. E.g. {@link java.util.Map}
 */
public class FirebaseSource<T>
  extends UnboundedSource<FirebaseEvent<T>, FirebaseCheckpoint<T>> {

  private static final long serialVersionUID = 5926529400127617094L;

  /**
   * Uses {@link FirebaseCheckpoint} to build a priority queue based on event time,
   * for events triggered by listeners specified in {@link FirebaseSource} construction.
   **/
  public class FirebaseReader extends UnboundedReader<FirebaseEvent<T>>{

    private final FirebaseSource<T> source;
    private final MessageDigest digest;
    private final Firebase timestampRef;
    private final FirebaseCheckpoint<T> checkpoint;
    private final Firebase ref;
    private ValueEventListener valueListener;
    private ChildEventListener childListener;
    private FirebaseError error = null;

    FirebaseReader(
        FirebaseSource<T> source,
        FirebaseCheckpoint<T> checkpoint) {
      this.source = source;
      try {
        this.digest = MessageDigest.getInstance("MD5");
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException(e);
      }
      if (checkpoint == null){
        this.checkpoint = new FirebaseCheckpoint<>();
      } else {
        this.checkpoint = checkpoint.reset();
      }
      this.ref = this.source.getRef();
      this.valueListener = makeValueListener();
      this.childListener = makeChildListener();
      this.timestampRef = this.ref.getRoot().child(
          Integer.toHexString(this.hashCode()) + Long.toHexString(System.currentTimeMillis()));
    }

    private ValueEventListener makeValueListener() {
      return new ValueEventListener(){

        @Override
        public void onCancelled(FirebaseError err) {
          error = err;
        }

        @Override
        public void onDataChange(DataSnapshot snapshot) {
          wrapEventWithInstant(new ValueChanged<T>(
              snapshot));
        }
      };
    }

    private ChildEventListener makeChildListener() {
      return new ChildEventListener(){
        @Override
        public void onCancelled(FirebaseError err) {
          error = err;
        }

        @Override
        public void onChildAdded(DataSnapshot snapshot, String previousChildName) {
          wrapEventWithInstant(new ChildAdded<T>(
              snapshot,
              previousChildName));
        }

        @Override
        public void onChildChanged(DataSnapshot snapshot, String previousChildName) {
          wrapEventWithInstant(new ChildChanged<T>(
              snapshot,
              previousChildName));
        }

        @Override
        public void onChildMoved(DataSnapshot snapshot, String previousChildName) {
          wrapEventWithInstant(new ChildMoved<T>(
              snapshot,
              previousChildName));
        }

        @Override
        public void onChildRemoved(DataSnapshot snapshot) {
          wrapEventWithInstant(new ChildRemoved<T>(
              snapshot));
        }
      };
    }

    /**
     * On receiving an event, {@link FirebaseReader} sets the {@link ServerValue#TIMESTAMP}
     * in the Firebase which will be populated with the current server time. This will trigger
     * a value change event, which can be used to retrieve a timestamp. Effectively this method
     * delegates dealing with clock drift between shards to the Firebase servers. Since events are
     * always processed in order by a single client, timestamps will be in the order events were
     * recieved.
     * @param event {@link FirebaseEvent} to wrap
     */
    private void wrapEventWithInstant(final FirebaseEvent<T> event){
      timestampRef.setValue(ServerValue.TIMESTAMP, new CompletionListener(){
        @Override
        public void onComplete(FirebaseError err, Firebase ref) {
          if (err != null){
            error = err;
          }
          ref.addListenerForSingleValueEvent(new ValueEventListener(){
            @Override
            public void onCancelled(FirebaseError err) {
              error = err;
            }
            @Override
            public void onDataChange(DataSnapshot timestamp) {
              checkpoint.put(new Record<>(
                  new Instant(timestamp.getValue(Long.class)),
                  event));
            }
          });
        }
      });
    }

    @Override
    public boolean advance() throws IOException {
      if (error == null){
        return this.checkpoint.advance();
      }
      throw error.toException();
    }

    @Override
    public CheckpointMark getCheckpointMark() {
      return checkpoint;
    }

    @Override
    public byte[] getCurrentRecordId() throws NoSuchElementException {
      return digest.digest(
          ByteBuffer.allocate(12).putInt(
              this.checkpoint.read().data.hashCode()).putLong(
                  this.checkpoint.read().timestamp.getMillis()).array());
    }

    @Override
    public UnboundedSource<FirebaseEvent<T>, ?> getCurrentSource() {
      return this.source;
    }

    @Override
    public Instant getWatermark() {
      return this.checkpoint.minTimestamp();
    }

    @Override
    public boolean start() throws IOException {
      try {
        this.source.auther.authenticate(this.ref);
      } catch (Exception e) {
        throw new IOException(e);
      }
      if (this.source.listenForChildEvents){
        this.childListener = this.ref.addChildEventListener(this.childListener);
      }
      if (this.source.listenForValueEvents){
        this.valueListener = this.ref.addValueEventListener(this.valueListener);
      }
      return this.advance();
    }

    @Override
    public void close() throws IOException {
      if (this.source.listenForChildEvents) {
        this.ref.removeEventListener(this.childListener);
      }
      if (this.source.listenForValueEvents){
        this.ref.removeEventListener(this.valueListener);
      }
      final CountDownLatch lock = new CountDownLatch(1);
      timestampRef.removeValue(new CompletionListener(){

        @Override
        public void onComplete(FirebaseError err, Firebase arg1) {
          lock.countDown();
          if (err != null){
            throw err.toException();
          }
        }
      });
      try {
        lock.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException();
      }
    }

    /**
     * @see com.google.cloud.dataflow.sdk.io.Source.Reader#getCurrent()
     **/
    @Override
    public FirebaseEvent<T> getCurrent() throws NoSuchElementException {
      if (this.checkpoint.read() == null){
        throw new NoSuchElementException();
      }
      return this.checkpoint.read().data;
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      if (this.checkpoint.read() == null){
        throw new NoSuchElementException();
      }
      return this.checkpoint.read().timestamp;
    }
  }

  private final String queryString;
  private final FirebaseAuthenticator auther;
  private final boolean listenForChildEvents;
  private final boolean listenForValueEvents;
  private final Class<T> clazz;
  private final Logger logger = LoggerFactory.getLogger(FirebaseSource.class);

  /**
   * @see FirebaseSource#of(Class, FirebaseAuthenticator, Query)
   * @return A FirebaseSource that uses only a {@link ChildEventListener}
   */
  public static <K> FirebaseSource<K> childListenerOf(
      Class<K> clazz,
      FirebaseAuthenticator auther,
      Query query){
    return new FirebaseSource<>(clazz, auther, query, true, false);
  }

  /**
   * @see FirebaseSource#of(Class, FirebaseAuthenticator, Query)
   * @return A FirebaseSource that uses only a {@link ValueEventListener}
   */
  public static <K> FirebaseSource<K> valueListenerOf(
      Class<K> clazz,
      FirebaseAuthenticator auther,
      Query query){
    return new FirebaseSource<>(clazz, auther, query, false, true);
  }

  /**
   * @param clazz determines the type of data the {@link DataSnapshot}s received from Firebase
   * servers will be unmarshalled into, and serialized and deserialized. Any POJO which is
   * serializable as a JSON Object by an {@link ObjectWriter} will work.
   * @param auther Used to authenticate to Firebase
   * @param query The {@link Query} which will create events passed on to the pipeline.
   * @return A FirebaseSource that uses both a {@link ChildEventListener} and a
   * {@link ValueEventListener}
   */
  public static <K> FirebaseSource<K> of(
      Class<K> clazz,
      FirebaseAuthenticator auther,
      Query query){
    return new FirebaseSource<>(clazz, auther, query, true, true);
  }

  /**
   * @param clazz Class Reference for the type to deserialize {@link DataSnapshot}s into.
   * @param auther Authenticator
   * @param query Any {@link Query} reference.
   * @param listenForChildEvents Adds a {@link ChildEventListener} during runtime
   * @param listenForValueEvents Adds a {@link ValueEventListener} during runtime
   */
  protected FirebaseSource(
      Class<T> clazz,
      FirebaseAuthenticator auther,
      Query query,
      boolean listenForChildEvents,
      boolean listenForValueEvents){
    if (query.toString().equals(query.getRef().getRoot().toString())){
      throw new IllegalArgumentException("Cannot use a reference to the root of a Firebase "
          + "repository. Since FirebaseSource uses a ref at the root of the repository to store"
          + "timestamps, this will result in an endless event loop, which will livelock your"
          + "pipeline.");
    }
    this.clazz = clazz;
    this.auther = auther;
    this.queryString = query.getRef().toString();
    this.listenForChildEvents = listenForChildEvents;
    this.listenForValueEvents = listenForValueEvents;
  }

  protected Firebase getRef(){
    return new Firebase(this.queryString);
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<? extends UnboundedSource<FirebaseEvent<T>, FirebaseCheckpoint<T>>>
    generateInitialSplits(
      int numSplits, PipelineOptions arg1) throws Exception {
    return Collections.singletonList(this);
  }

  @Override
  public Coder<FirebaseEvent<T>> getDefaultOutputCoder() {
    return FirebaseEventCoder.of(new TypeDescriptor<FirebaseEvent<T>>(){}, this.clazz);
  }

  @Override
  public void validate() {
    AuthData result;
    try {
      result = auther.authenticate(this.getRef());
    } catch (FirebaseException e) {
      throw new RuntimeException("Authenticator failed to authenticate", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Thread interrupted while waiting for authentication to complete", e);
    }
    checkNotNull(result, "If result is null, authenticators is ");
  }

  @Override
  public UnboundedReader<FirebaseEvent<T>> createReader(
      PipelineOptions arg0, @Nullable FirebaseCheckpoint<T> arg1) {
    return new FirebaseReader(this, arg1);
  }

  @Override
  public Coder<FirebaseCheckpoint<T>> getCheckpointMarkCoder() {
    return FirebaseCheckpointCoder.of(new TypeDescriptor<FirebaseCheckpoint<T>>(){}, this.clazz);
  }

}
