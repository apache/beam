/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.core.executor.io;

import cz.seznam.euphoria.shadow.com.google.common.collect.Lists;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Before;

public class FsSpillingListStorageTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  TmpFolderSpillFileFactory spillFiles;

  @Before
  public void setUp() {
    spillFiles = new TmpFolderSpillFileFactory(folder);
  }

  @Test
  public void testAddMaxElemsDoesNotSpill() {
    FsSpillingListStorage<String> storage = new FsSpillingListStorage<>(
        new JavaSerializationFactory(), spillFiles, 3);

    // ~ add exactly 3 elements
    storage.add("foo");
    storage.add("bar");
    storage.add("quux");

    // ~ verify we can read them again (and repeatedly given the one iterable)
    Iterable<String> elements = storage.get();
    assertEquals(Arrays.asList("foo", "bar", "quux"), Lists.newArrayList(elements));
    assertEquals(Arrays.asList("foo", "bar", "quux"), Lists.newArrayList(elements));
    // ~ verify no spill files was created
    assertEquals(Collections.emptyList(), spillFiles.getCreatedFiles());
    // ~ verify clear does not fail
    storage.clear();
  }

  @Test
  public void testAddOneExactSpill() {
    FsSpillingListStorage<String> storage = new FsSpillingListStorage<>(
        new JavaSerializationFactory(), spillFiles, 3);
    storage.addAll(Arrays.asList("one", "two", "three", "four"));

    // ~ assert the data was spilled
    assertEquals(1, spillFiles.getCreatedFiles().size());
    assertTrue(spillFiles.getCreatedFiles().get(0).exists());

    // ~ assert we can read the content (repeatedly)
    Iterable<String> elements = storage.get();
    assertEquals(
        Arrays.asList("one", "two", "three", "four"),
        Lists.newArrayList(elements));

    assertEquals(
        Arrays.asList("one", "two", "three", "four"),
        Lists.newArrayList(elements));

    // ~ assert that the spill files get properly cleaned up
    storage.clear();
    assertFalse(spillFiles.getCreatedFiles().get(0).exists());
  }

  @Test
  public void testMixedIteration() {
    List<String> input = Arrays.asList(
        "one", "two", "three", "four", "five", "six", "seven", "eight");


    FsSpillingListStorage<String> storage =
        new FsSpillingListStorage<>(new JavaSerializationFactory(), spillFiles, 5);
    storage.addAll(input);

    // ~ assert the data was spilled
    assertEquals(1, spillFiles.getCreatedFiles().size());
    assertTrue(spillFiles.getCreatedFiles().get(0).exists());

    // ~ assert we can read the content (repeatedly and concurrently)
    Iterable<String> elements = storage.get();
    Iterator<String> first = elements.iterator();
    Iterator<String> second = elements.iterator();

    // ~ try to read the two iterators interleaved (give first a small advantage)
    assertEquals(input.get(0), first.next());
    assertEquals(input.get(1), first.next());
    for (int i = 0; i < input.size(); i++) {
      if (i+2 < input.size()) {
        assertEquals(input.get(i + 2), first.next());
      }
      assertEquals(input.get(i), second.next());
    }
    assertFalse(first.hasNext());
    assertFalse(second.hasNext());

    // ~ assert that the spill files get properly cleaned up
    storage.clear();
    assertFalse(spillFiles.getCreatedFiles().get(0).exists());
  }

  @Test
  public void testCloseOutput() {
    List<String> input = Arrays.asList(
        "one", "two", "three", "four", "five", "six", "seven", "eight");


    FsSpillingListStorage<String> storage =
        new FsSpillingListStorage<>(new JavaSerializationFactory(), spillFiles, 5);
    storage.addAll(input);
    storage.closeOutput();
    assertEquals(input, StreamSupport
        .stream(storage.get().spliterator(), false)
        .collect(Collectors.toList()));
  }
}