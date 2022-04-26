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
package org.apache.beam.fn.harness.jmh;

import org.apache.beam.fn.harness.jmh.ProcessBundleBenchmark.StatefulTransform;
import org.apache.beam.fn.harness.jmh.ProcessBundleBenchmark.TrivialTransform;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ProcessBundleBenchmark}. */
@RunWith(JUnit4.class)
public class ProcessBundleBenchmarkTest {
  @Test
  public void testTinyBundle() throws Exception {
    TrivialTransform transform = new TrivialTransform();
    new ProcessBundleBenchmark().testTinyBundle(transform);
    transform.tearDown();
  }

  @Test
  public void testLargeBundle() throws Exception {
    TrivialTransform transform = new TrivialTransform();
    new ProcessBundleBenchmark().testLargeBundle(transform);
    transform.tearDown();
  }

  @Test
  public void testStateWithoutCaching() throws Exception {
    StatefulTransform transform = new StatefulTransform();
    new ProcessBundleBenchmark().testStateWithoutCaching(transform);
    transform.tearDown();
  }

  @Test
  public void testStateWithCaching() throws Exception {
    StatefulTransform transform = new StatefulTransform();
    new ProcessBundleBenchmark().testStateWithCaching(transform);
    transform.tearDown();
  }
}
