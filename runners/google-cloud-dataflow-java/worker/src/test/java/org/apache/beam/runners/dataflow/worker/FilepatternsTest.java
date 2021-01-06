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
package org.apache.beam.runners.dataflow.worker;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItems;

import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link Filepatterns}. */
@RunWith(JUnit4.class)
public class FilepatternsTest {
  @Rule public ExpectedException exception = ExpectedException.none();

  @Test
  public void testExpandAtNFilepatternNoPattern() throws Exception {
    assertThat(
        Filepatterns.expandAtNFilepattern("gs://bucket/object@google.ism"),
        contains("gs://bucket/object@google.ism"));
  }

  @Test
  public void testExpandAtNFilepatternTwoPatterns() throws Exception {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(
        "More than one @N wildcard found in filepattern: gs://bucket/object@10.@20.ism");
    Filepatterns.expandAtNFilepattern("gs://bucket/object@10.@20.ism");
  }

  @Test
  public void testExpandAtNFilepatternHugeN() throws Exception {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(
        "Unsupported number of shards: 2000000000 "
            + "in filepattern: gs://bucket/object@2000000000.ism");
    Filepatterns.expandAtNFilepattern("gs://bucket/object@2000000000.ism");
  }

  @Test
  public void testExpandAtNFilepatternSmall() throws Exception {
    assertThat(
        Filepatterns.expandAtNFilepattern("gs://bucket/file@2.ism"),
        contains("gs://bucket/file-00000-of-00002.ism", "gs://bucket/file-00001-of-00002.ism"));
  }

  @Test
  public void testExpandAtNFilepatternLarge() throws Exception {
    Iterable<String> iterable = Filepatterns.expandAtNFilepattern("gs://bucket/file@200000.ism");
    assertThat(iterable, Matchers.<String>iterableWithSize(200000));
    assertThat(
        iterable,
        hasItems("gs://bucket/file-003232-of-200000.ism", "gs://bucket/file-199999-of-200000.ism"));
  }
}
