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
package org.apache.beam.sdk.runners.inprocess;

import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.theInstance;
import static org.junit.Assert.assertThat;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.TextIOTest;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;

/**
 * Tests for {@link TextIOShardedWriteFactory}.
 */
@RunWith(JUnit4.class)
public class TextIOShardedWriteFactoryTest {
  @Rule public TemporaryFolder tmp = new TemporaryFolder();
  private TextIOShardedWriteFactory factory;

  @Before
  public void setup() {
    factory = new TextIOShardedWriteFactory();
  }

  @Test
  public void originalWithoutShardingReturnsOriginal() throws Exception {
    File file = tmp.newFile("foo");
    PTransform<PCollection<String>, PDone> original =
        TextIO.Write.to(file.getAbsolutePath()).withoutSharding();
    PTransform<PCollection<String>, PDone> overridden = factory.override(original);

    assertThat(overridden, theInstance(original));
  }

  @Test
  public void originalShardingNotSpecifiedReturnsOriginal() throws Exception {
    File file = tmp.newFile("foo");
    PTransform<PCollection<String>, PDone> original = TextIO.Write.to(file.getAbsolutePath());
    PTransform<PCollection<String>, PDone> overridden = factory.override(original);

    assertThat(overridden, theInstance(original));
  }

  @Test
  public void originalShardedToOneReturnsExplicitlySharded() throws Exception {
    File file = tmp.newFile("foo");
    TextIO.Write.Bound<String> original =
        TextIO.Write.to(file.getAbsolutePath()).withNumShards(1);
    PTransform<PCollection<String>, PDone> overridden = factory.override(original);

    assertThat(overridden, not(Matchers.<PTransform<PCollection<String>, PDone>>equalTo(original)));

    TestPipeline p = TestPipeline.create();
    String[] elems = new String[] {"foo", "bar", "baz"};
    p.apply(Create.<String>of(elems)).apply(overridden);

    file.delete();

    p.run();
    TextIOTest.assertOutputFiles(
        elems, StringUtf8Coder.of(), 1, tmp, "foo", original.getShardNameTemplate());
  }

  @Test
  public void originalShardedToManyReturnsExplicitlySharded() throws Exception {
    File file = tmp.newFile("foo");
    TextIO.Write.Bound<String> original = TextIO.Write.to(file.getAbsolutePath()).withNumShards(3);
    PTransform<PCollection<String>, PDone> overridden = factory.override(original);

    assertThat(overridden, not(Matchers.<PTransform<PCollection<String>, PDone>>equalTo(original)));

    TestPipeline p = TestPipeline.create();
    String[] elems = new String[] {"foo", "bar", "baz", "spam", "ham", "eggs"};
    p.apply(Create.<String>of(elems)).apply(overridden);

    file.delete();
    p.run();
    TextIOTest.assertOutputFiles(
        elems, StringUtf8Coder.of(), 3, tmp, "foo", original.getShardNameTemplate());
  }
}
