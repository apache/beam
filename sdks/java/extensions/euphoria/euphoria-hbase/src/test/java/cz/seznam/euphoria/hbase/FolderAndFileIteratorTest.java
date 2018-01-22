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
package cz.seznam.euphoria.hbase;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import cz.seznam.euphoria.hbase.util.RecursiveAllPathIterator;

public class FolderAndFileIteratorTest {

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test(expected = FileNotFoundException.class)
  public void exception() throws IOException {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    Path path = new Path("/not/exisitng/path/at/all");
    new RecursiveAllPathIterator(fs, path);
  }

  @Test
  public void emptyFolder() throws IOException {
    File fa = tmpFolder.newFolder("empty-root");
    FileSystem fs = FileSystem.getLocal(new Configuration());
    Path path = new Path(fa.getAbsolutePath());
    RecursiveAllPathIterator k = new RecursiveAllPathIterator(fs, path);
    List<String> list = new ArrayList<>();
    while (k.hasNext()) {
      String s = k.next().getName();
      list.add(s);
    }
    assertTrue("empty-root".equals(list.get(0)));
  }

  @Test
  public void structure() throws IOException {
    File fa = tmpFolder.newFolder("folder-a");
    FileSystem fs = FileSystem.getLocal(new Configuration());
    fs.createNewFile(new Path(fa.getAbsolutePath() + "/file-a"));
    fs.createNewFile(new Path(fa.getAbsolutePath() + "/file-b"));
    fs.mkdirs(new Path(fa.getAbsolutePath() + "/empty"));

    Path path = new Path(fa.getAbsolutePath());
    RecursiveAllPathIterator k = new RecursiveAllPathIterator(fs, path);
    List<String> expected = asList("folder-a", "file-a", "file-b", "empty");
    Set<String> set = new HashSet<>();
    while (k.hasNext()) {
      String s = k.next().getName();
      set.add(s);
    }
    expected.forEach(ex -> set.contains(ex));
  }

}
