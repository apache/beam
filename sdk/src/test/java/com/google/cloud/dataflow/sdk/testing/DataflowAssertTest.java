/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.testing;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.AtomicCoder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObserver;
import com.google.cloud.dataflow.sdk.values.PCollection;

import com.fasterxml.jackson.annotation.JsonCreator;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * Test case for {@link DataflowAssert}.
 */
@RunWith(JUnit4.class)
public class DataflowAssertTest implements Serializable {

  @Rule
  public transient ExpectedException thrown = ExpectedException.none();

  private static class NotSerializableObject {

    @Override
    public boolean equals(Object other) {
      return (other instanceof NotSerializableObject);
    }

    @Override
    public int hashCode() {
      return 73;
    }
  }

  private static class NotSerializableObjectCoder extends AtomicCoder<NotSerializableObject> {
    private NotSerializableObjectCoder() { }
    private static final NotSerializableObjectCoder INSTANCE = new NotSerializableObjectCoder();

    @JsonCreator
    public static NotSerializableObjectCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(NotSerializableObject value, OutputStream outStream, Context context)
        throws CoderException, IOException {
    }

    @Override
    public NotSerializableObject decode(InputStream inStream, Context context)
        throws CoderException, IOException {
      return new NotSerializableObject();
    }

    @Override
    public boolean isDeterministic() {
      return true;
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(NotSerializableObject value, Context context) {
      return true;
    }

    @Override
    public void registerByteSizeObserver(
        NotSerializableObject value, ElementByteSizeObserver observer, Context context)
        throws Exception {
      observer.update(0L);
    }
  }

  /**
   * A {@link DataflowAssert} about the contents of a {@link PCollection}
   * must not require the contents of the {@link PCollection} to be
   * serializable.
   */
  @Test
  @Category(com.google.cloud.dataflow.sdk.testing.RunnableOnService.class)
  public void testContainsInAnyOrderNotSerializable() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    PCollection<NotSerializableObject> pcollection = pipeline
        .apply(Create.of(
          new NotSerializableObject(),
          new NotSerializableObject()))
        .setCoder(NotSerializableObjectCoder.of());

    DataflowAssert.that(pcollection).containsInAnyOrder(
      new NotSerializableObject(),
      new NotSerializableObject());

    pipeline.run();
  }

  /**
   * A {@link DataflowAssert} about the contents of a {@link PCollection}
   * is allows to be verified by an arbitrary {@link SerializableFunction},
   * though.
   */
  @Test
  @Category(com.google.cloud.dataflow.sdk.testing.RunnableOnService.class)
  public void testSerializablePredicate() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    PCollection<NotSerializableObject> pcollection = pipeline
        .apply(Create.of(
          new NotSerializableObject(),
          new NotSerializableObject()))
        .setCoder(NotSerializableObjectCoder.of());

    DataflowAssert.that(pcollection).satisfies(
        new SerializableFunction<Iterable<NotSerializableObject>, Void>() {
          @Override
          public Void apply(Iterable<NotSerializableObject> contents) {
            return (Void) null; // no problem!
          }
        });

    pipeline.run();
  }


  @Test
  @Category(com.google.cloud.dataflow.sdk.testing.RunnableOnService.class)
  public void testIsEqualTo() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    PCollection<Integer> pcollection = pipeline
        .apply(Create.of(43));

    DataflowAssert.thatSingleton(pcollection).isEqualTo(43);

    pipeline.run();
  }

  @Test
  @Category(com.google.cloud.dataflow.sdk.testing.RunnableOnService.class)
  public void testContainsInAnyOrder() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    PCollection<Integer> pcollection = pipeline
        .apply(Create.of(1, 2, 3, 4));

    DataflowAssert.that(pcollection).containsInAnyOrder(2, 1, 4, 3);

    pipeline.run();
  }

  @Test
  @Category(com.google.cloud.dataflow.sdk.testing.RunnableOnService.class)
  public void testContainsInAnyOrderFalse() throws Exception {
    // The actual AssertionError is deep in the stack
    // TODO: dig it out
    thrown.expect(RuntimeException.class);

    Pipeline pipeline = TestPipeline.create();

    PCollection<Integer> pcollection = pipeline
        .apply(Create.of(1, 2, 3, 4));

    DataflowAssert.that(pcollection).containsInAnyOrder(2, 1, 4, 3, 7);

    pipeline.run();
  }


  @Test
  @Category(com.google.cloud.dataflow.sdk.testing.RunnableOnService.class)
  public void testContainsInOrder() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    PCollection<Integer> pcollection = pipeline
        .apply(Create.of(1, 2, 3, 4))
        .setOrdered(true);

    DataflowAssert.that(pcollection).containsInOrder(1, 2, 3, 4);

    pipeline.run();
  }

  @Test
  @Category(com.google.cloud.dataflow.sdk.testing.RunnableOnService.class)
  public void testContainsInOrderFalse() throws Exception {
    // The actual AssertionError is deep in the stack
    // TODO: dig it out
    thrown.expect(RuntimeException.class);

    Pipeline pipeline = TestPipeline.create();

    PCollection<Integer> pcollection = pipeline
        .apply(Create.of(1, 2, 3, 4))
        .setOrdered(true);

    DataflowAssert.that(pcollection).containsInOrder(1, 2, 4, 3);

    pipeline.run();
  }
}
