/**
 * Copyright (c) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.contrib.firebase.io;

import com.google.cloud.dataflow.contrib.firebase.contrib.LogElements;
import com.google.cloud.dataflow.contrib.firebase.events.FirebaseEvent;
import com.google.cloud.dataflow.contrib.firebase.utils.FirebaseAuthenticator;
import com.google.cloud.dataflow.contrib.firebase.utils.FirebaseEmptyAuthenticator;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.values.PCollection;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebase.client.ChildEventListener;
import com.firebase.client.Firebase;
import com.firebase.client.Firebase.CompletionListener;
import com.firebase.client.FirebaseError;
import com.firebase.client.ValueEventListener;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;



/**
 * Archetype for comparing events produced by through {@link FirebaseSource} with events surfaced
 * by the unmodified {@link ChildEventListener} and {@link ValueEventListener}.
 **/
@RunWith(JUnit4.class)
public abstract class BaseFirebaseSourceTest {

  /**
   * Logs all the added entries!
  **/
  private class LoggingArrayList<T> extends ArrayList<T> {
    private Logger logger;

    LoggingArrayList(Logger logger){
      this.logger = logger;
    }

    @Override
    public boolean add(T e){
      logger.info(e.toString());
      return super.add(e);
    }

  }

  FirebaseAuthenticator auther = new FirebaseEmptyAuthenticator();
  private Throwable err;
  Firebase testRef;

  static final Logger LOGGER = LoggerFactory.getLogger(BaseFirebaseSourceTest.class);

  private final LoggingArrayList<FirebaseEvent<JsonNode>> expected =
      new LoggingArrayList<>(LOGGER);

  @Before
  public void setUp() throws IOException, InterruptedException {

    testRef = new Firebase("https://dataflowio.firebaseio-demo.com")
        .child(Integer.toHexString(this.hashCode())
            + Long.toHexString(System.currentTimeMillis()));

    cleanFirebase(testRef);

    prepareData(new ObjectMapper().<List<Map<String, Object>>>readValue(
        ClassLoader.getSystemResourceAsStream("testdata.json"),
        new TypeReference<List<Object>>() {}));

  }

  @After
  public void teardown() throws InterruptedException{
    cleanFirebase(testRef);
  }

  private Thread getPipelineThread(final Pipeline p){
    Thread t = new Thread(new Runnable(){
      @Override
      public void run() {
        LOGGER.info(p.run().getState().toString());
      }
    });
    t.setUncaughtExceptionHandler(new UncaughtExceptionHandler(){
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        err = e;
      }
    });
    return t;
  }

  private void cleanFirebase(Firebase f) throws InterruptedException{
    final CountDownLatch lock = new CountDownLatch(1);
    f.removeValue(new CompletionListener(){
      @Override
      public void onComplete(FirebaseError arg0, Firebase arg1) {
        lock.countDown();
      }
    });
    lock.await();
  }

  public abstract void prepareData(List<Map<String, Object>> testData);

  public abstract void triggerEvents(Firebase f);

  public abstract void addListener(Firebase f, final Collection<FirebaseEvent<JsonNode>> events);

  public abstract void removeListener(Firebase f);

  public abstract FirebaseSource<JsonNode> makeSource(FirebaseAuthenticator auther,
      Firebase f);

  @Test
  public void matchEvents() throws Throwable{

    //Generate expected events
    addListener(testRef, expected);
    triggerEvents(testRef);
    cleanFirebase(testRef);
    removeListener(testRef);
    LOGGER.info("At most " + expected.size() + " elements to look for in pipeline");


    FirebaseSource<JsonNode> source = makeSource(auther, testRef);

    TestPipeline p = TestPipeline.create();

    PCollection<FirebaseEvent<JsonNode>> events = p
        .apply(Read.from(source).withMaxNumRecords(expected.size()));

    events.apply(new LogElements<FirebaseEvent<JsonNode>>(FirebaseChildTest.class, Level.INFO));

    DataflowAssert.that(events).containsInAnyOrder(expected);

    //Test pipeline against expected events;
    Thread t = getPipelineThread(p);
    t.start();
    Thread.sleep(1500);
    triggerEvents(testRef);
    cleanFirebase(testRef);
    t.join();
    if (err != null){
      throw err;
    }
  }

}
