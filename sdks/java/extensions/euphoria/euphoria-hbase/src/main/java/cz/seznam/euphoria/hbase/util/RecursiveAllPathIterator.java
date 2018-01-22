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
package cz.seznam.euphoria.hbase.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Stack;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

/**
 * Traverses all paths under rootFolder including files,folders and root folder itself
 */
public class RecursiveAllPathIterator implements RemoteIterator<Path> {

  private Stack<Path> itors = new Stack<Path>();
  private FileSystem fs;

  public RecursiveAllPathIterator(FileSystem fs, Path path) throws IOException {
    this.fs = fs;
    if (!fs.exists(path)) {
      throw new FileNotFoundException("Path " + path + " doesn't exists. Can't create iterator");
    }
    itors.push(path);
  }

  @Override
  public Path next() throws IOException {
    if (hasNext()) {
      Path currentPath = itors.pop();
      if (fs.isDirectory(currentPath)) {
        RemoteIterator<LocatedFileStatus> it = fs.listLocatedStatus(currentPath);
        while (it.hasNext()) {
          itors.push(it.next().getPath());
        }
      }
      return currentPath;
    }
    throw new java.util.NoSuchElementException("No more entry");
  }

  @Override
  public boolean hasNext() throws IOException {
    return !itors.isEmpty();
  }

}
