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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import cz.seznam.euphoria.core.client.io.ExternalIterable;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Test suite for {@code GenericSpillTools}. */
public class GenericSpillToolsTest {

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  private TmpFolderSpillFileFactory spillFiles;
  private GenericSpillTools tools;

  @Before
  public void setUp() {
    spillFiles = new TmpFolderSpillFileFactory(folder);
    tools = new GenericSpillTools(new JavaSerializationFactory(), spillFiles, 100);
  }

  @Test
  public void testSorted() throws InterruptedException {
    Iterable<Integer> iterable =
        tools.sorted(
            IntStream.range(0, 1003)
                .boxed()
                // sort descending
                .sorted(Comparator.reverseOrder())
                .collect(Collectors.toList()),
            Integer::compare);

    List<Integer> sorted =
        StreamSupport.stream(iterable.spliterator(), false).collect(Collectors.toList());

    assertEquals(1003, sorted.stream().distinct().count());
    checkSorted(sorted);
  }

  @Test
  public void testSpilling() throws InterruptedException {
    Collection<ExternalIterable<Integer>> parts =
        tools.spillAndSortParts(
            IntStream.range(0, 1003)
                .boxed()
                // sort descending
                .sorted(Comparator.reverseOrder())
                .collect(Collectors.toList()),
            Integer::compare);

    // 11 = ceil(1003 / 100)
    assertEquals(11, parts.size());
    assertEquals(11, spillFiles.getCreatedFiles().size());
  }

  private void checkSorted(List<Integer> input) {
    int last = Integer.MIN_VALUE;
    for (Integer i : input) {
      assertTrue(
          "Last element was " + last + " next was not greater or equals, was " + i, last <= i);
      last = i;
    }
  }
}
