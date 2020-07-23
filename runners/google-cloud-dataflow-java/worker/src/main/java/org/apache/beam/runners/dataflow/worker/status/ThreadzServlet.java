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
package org.apache.beam.runners.dataflow.worker.status;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.beam.runners.dataflow.worker.status.DebugCapture.Capturable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Respond to /threadz with the stack traces of all running threads. */
class ThreadzServlet extends BaseStatusServlet implements Capturable {

  public ThreadzServlet() {
    super("threadz");
  }

  /**
   * Class representing the execution state of a thread.
   *
   * <p>Can be used in hash maps.
   */
  @VisibleForTesting
  static class Stack {
    final StackTraceElement[] elements;
    final Thread.State state;

    Stack(StackTraceElement[] elements, Thread.State state) {
      this.elements = elements;
      this.state = state;
    }

    @Override
    public int hashCode() {
      return Objects.hash(Arrays.deepHashCode(elements), state);
    }

    @Override
    public boolean equals(@Nullable Object other) {
      if (other == this) {
        return true;
      } else if (!(other instanceof Stack)) {
        return false;
      } else {
        Stack that = (Stack) other;
        return state == that.state && Arrays.deepEquals(elements, that.elements);
      }
    }
  }

  @VisibleForTesting
  static Map<Stack, List<String>> deduplicateThreadStacks(
      Map<Thread, StackTraceElement[]> allStacks) {
    Map<Stack, List<String>> stacks = new HashMap<>();
    for (Map.Entry<Thread, StackTraceElement[]> entry : allStacks.entrySet()) {
      Thread thread = entry.getKey();
      if (thread != Thread.currentThread()) {
        Stack stack = new Stack(entry.getValue(), thread.getState());
        List<String> threads = stacks.get(stack);
        if (threads == null) {
          threads = new ArrayList<>();
          stacks.put(stack, threads);
        }
        threads.add(thread.toString());
      }
    }
    return stacks;
  }

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    response.setContentType("text/plain;charset=utf-8");
    response.setStatus(HttpServletResponse.SC_OK);
    captureData(response.getWriter());
  }

  @Override
  public String pageName() {
    return "/threadz";
  }

  @Override
  public void captureData(PrintWriter writer) {
    // First, build a map of stacks to the threads that have that stack.
    Map<Stack, List<String>> stacks = deduplicateThreadStacks(Thread.getAllStackTraces());

    // Then, print out each stack along with the threads that share it. Stacks with more threads
    // are printed first.
    stacks.entrySet().stream()
        .sorted(Comparator.comparingInt(e -> -e.getValue().size()))
        .forEachOrdered(
            entry -> {
              Stack stack = entry.getKey();
              List<String> threads = entry.getValue();
              writer.println(
                  "--- Threads ("
                      + threads.size()
                      + "): "
                      + threads
                      + " State: "
                      + stack.state
                      + " stack: ---");
              for (StackTraceElement element : stack.elements) {
                writer.println("  " + element);
              }
              writer.println();
            });
  }
}
