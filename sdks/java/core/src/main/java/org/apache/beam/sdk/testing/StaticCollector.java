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
package org.apache.beam.sdk.testing;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * StaticCollector gathers elements in a static variable where they can be easily accessed in the
 * test harness for an integration test. It is useful when testing streaming pipelines and PAssert
 * is to constrained.
 *
 * <p>Usage:
 *
 * <pre><code>
 * {@literal @Rule}
 *  public final transient StaticCollector&lt;String&gt collector = new StaticCollector&lt;&gt;();
 *
 * {@literal @Test}
 *  public void myPipelineTest() throws Exception {
 *    final PCollection&lt;String&gt; pCollection = ...;
 *    pCollection.apply(collector.collecting());
 *    pipeline.run();
 *    while (true) {
 *      Thread.sleep(500);
 *      if (ImmutableSet.copyOf(collector.snapshot()).equals(ImmutableSet.of("a", "b"))) {
 *        return;
 *      }
 *    }
 *  }
 * </code></pre>
 */
@Experimental
public final class StaticCollector<T> implements TestRule {
  @GuardedBy("StaticCollector")
  private static @Nullable Statement currentStatement;

  @GuardedBy("StaticCollector")
  private static final List<Object> objects = new ArrayList<>();

  private static synchronized void setUp(Statement statement) {
    checkState(
        currentStatement == null,
        "Multiple tests running concurrently not supported by StaticCollector.");
    currentStatement = statement;
    objects.clear();
  }

  private static synchronized void tearDown(Statement statement) {
    checkState(currentStatement == statement);
    objects.clear();
    currentStatement = null;
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        setUp(base);
        try {
          base.evaluate();
        } finally {
          tearDown(base);
        }
      }
    };
  }

  private static synchronized void addInternal(Object object) {
    objects.add(object);
  }

  /** Add an element to the collector. */
  public void add(T t) {
    addInternal(t);
  }

  private static synchronized List<Object> snapshotInternal() {
    return ImmutableList.copyOf(objects);
  }

  /** Snapshot the current state of the collector. */
  public List<T> snapshot() {
    return (List<T>) snapshotInternal();
  }

  /** A PTransform which adds any elements it sees to the collector. */
  public <U extends T> PTransform<PCollection<U>, PCollection<Void>> collecting() {
    return new PTransform<PCollection<U>, PCollection<Void>>() {
      @Override
      public PCollection<Void> expand(PCollection<U> input) {
        return input.apply(
            MapElements.via(
                new SimpleFunction<U, Void>() {
                  @Override
                  public Void apply(U input) {
                    add(input);
                    return null;
                  }
                }));
      }
    };
  }
}
