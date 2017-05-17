package cz.seznam.euphoria.core.executor.storage;

import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Lists;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FsSpillingListStorageTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  class TmpFolderSpillFileFactory implements FsSpillingListStorage.SpillFileFactory {
    final List<File> served = new ArrayList<>();
    @Override
    public File newSpillFile() {
      try {
        File f = folder.newFile();
        served.add(f);
        return f;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test
  public void testAddMaxElemsDoesNotSpill() {
    TmpFolderSpillFileFactory spillFiles = new TmpFolderSpillFileFactory();
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
    assertEquals(Collections.emptyList(), spillFiles.served);
    // ~ verify clear does not fail
    storage.clear();
  }

  @Test
  public void testAddOneExactSpill() {
    TmpFolderSpillFileFactory spillFiles = new TmpFolderSpillFileFactory();
    FsSpillingListStorage<String> storage = new FsSpillingListStorage<>(
        new JavaSerializationFactory(), spillFiles, 3);
    storage.addAll(Arrays.asList("one", "two", "three", "four"));

    // ~ assert the data was spilled
    assertEquals(1, spillFiles.served.size());
    assertTrue(spillFiles.served.get(0).exists());

    // ~ assert we can read the content (repeatedly)
    Iterable<String> elements = storage.get();
    assertEquals(Arrays.asList("one", "two", "three", "four"), Lists.newArrayList(elements));
    assertEquals(Arrays.asList("one", "two", "three", "four"), Lists.newArrayList(elements));

    // ~ assert that the spill files get properly cleaned up
    storage.clear();
    assertFalse(spillFiles.served.get(0).exists());
  }

  @Test
  public void testMixedIteration() {
    List<String> input = Arrays.asList("one", "two", "three", "four", "five", "six", "seven", "eight");

    TmpFolderSpillFileFactory spillFiles = new TmpFolderSpillFileFactory();
    FsSpillingListStorage<String> storage =
        new FsSpillingListStorage<>(new JavaSerializationFactory(), spillFiles, 5);
    storage.addAll(input);

    // ~ assert the data was spilled
    assertEquals(1, spillFiles.served.size());
    assertTrue(spillFiles.served.get(0).exists());

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
    assertFalse(spillFiles.served.get(0).exists());
  }
}