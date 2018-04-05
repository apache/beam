/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.test;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.util.List;
import java.util.stream.Stream;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Tracks the threads created during a test method execution (or class using @ClassRule)
 * and fails if some still exists after the test method execution.
 */
public class ThreadLeakTracker implements TestRule {
  private final Field groupField;

  public ThreadLeakTracker() {
    try {
      groupField = Thread.class.getDeclaredField("group");
      if (!groupField.isAccessible()) {
        groupField.setAccessible(true);
      }
    } catch (final NoSuchFieldException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public Statement apply(final Statement base, final Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        final Thread thread = Thread.currentThread();
        final ThreadGroup threadGroup = thread.getThreadGroup();
        final ThreadGroup testGroup = new ThreadGroup(threadGroup, Long.toString(thread.getId()));
        groupField.set(thread, testGroup);
        try {
          base.evaluate();
        } finally {
          groupField.set(thread, threadGroup);
          final Thread[] threads = listThreads();
          final List<Thread> leaked = Stream.of(threads)
            .filter(t -> t.getThreadGroup() == testGroup)
            .collect(toList());
          if (!leaked.isEmpty()) {
            fail("Some threads leaked: " + leaked.stream().map(Thread::getName)
              .collect(joining("\n- ", "\n- ", "")));
          }
        }
      }
    };
  }

  private Thread[] listThreads() {
    final int count = Thread.activeCount();
    final Thread[] threads = new Thread[Math.max(count * 2, 16)];
    final int threadCount = Thread.enumerate(threads);
    final Thread[] array = new Thread[threadCount];
    System.arraycopy(threads, 0, array, 0, threadCount);
    return array;
  }
}
