/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.spark.structuredstreaming.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/** A {@code SerializationDebugger} for Spark Runner. */
public class SerializationDebugger {

  public static void testSerialization(Object object, File to) throws IOException {
    DebuggingObjectOutputStream out = new DebuggingObjectOutputStream(new FileOutputStream(to));
    try {
      out.writeObject(object);
    } catch (Exception e) {
      throw new RuntimeException("Serialization error. Path to bad object: " + out.getStack(), e);
    }
  }

  private static class DebuggingObjectOutputStream extends ObjectOutputStream {

    private static final Field DEPTH_FIELD;

    static {
      try {
        DEPTH_FIELD = ObjectOutputStream.class.getDeclaredField("depth");
        DEPTH_FIELD.setAccessible(true);
      } catch (NoSuchFieldException e) {
        throw new AssertionError(e);
      }
    }

    final List<Object> stack = new ArrayList<>();

    /**
     * Indicates whether or not OOS has tried to write an IOException (presumably as the result of a
     * serialization error) to the stream.
     */
    boolean broken = false;

    DebuggingObjectOutputStream(OutputStream out) throws IOException {
      super(out);
      enableReplaceObject(true);
    }

    /** Abuse {@code replaceObject()} as a hook to maintain our stack. */
    @Override
    protected Object replaceObject(Object o) {
      // ObjectOutputStream writes serialization
      // exceptions to the stream. Ignore
      // everything after that so we don't lose
      // the path to a non-serializable object. So
      // long as the user doesn't write an
      // IOException as the root object, we're OK.
      int currentDepth = currentDepth();
      if (o instanceof IOException && currentDepth == 0) {
        broken = true;
      }
      if (!broken) {
        truncate(currentDepth);
        stack.add(o);
      }
      return o;
    }

    private void truncate(int depth) {
      while (stack.size() > depth) {
        pop();
      }
    }

    private Object pop() {
      return stack.remove(stack.size() - 1);
    }

    /** Returns a 0-based depth within the object graph of the current object being serialized. */
    private int currentDepth() {
      try {
        Integer oneBased = ((Integer) DEPTH_FIELD.get(this));
        return oneBased - 1;
      } catch (IllegalAccessException e) {
        throw new AssertionError(e);
      }
    }

    /**
     * Returns the path to the last object serialized. If an exception occurred, this should be the
     * path to the non-serializable object.
     */
    List<Object> getStack() {
      return stack;
    }
  }
}
