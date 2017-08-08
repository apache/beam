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
package org.apache.beam.runners.mapreduce.translation;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;

/**
 * Files based {@link SideInputReader}.
 */
public class FileSideInputReader implements SideInputReader {

  private final Map<TupleTag<?>, String> tupleTagToFilePath;
  private final Map<TupleTag<?>, Coder<?>> tupleTagToCoder;
  private final Configuration conf;

  public FileSideInputReader(
      Map<TupleTag<?>, String> tupleTagToFilePath,
      Map<TupleTag<?>, Coder<?>> tupleTagToCoder,
      Configuration conf) {
    this.tupleTagToFilePath = checkNotNull(tupleTagToFilePath, "tupleTagToFilePath");
    this.tupleTagToCoder = checkNotNull(tupleTagToCoder, "tupleTagToCoder");
    this.conf = checkNotNull(conf, "conf");
  }

  @Nullable
  @Override
  public <T> T get(PCollectionView<T> view, BoundedWindow window) {
    String filePath = tupleTagToFilePath.get(view.getTagInternal());
    IterableCoder<WindowedValue<?>> coder =
        (IterableCoder<WindowedValue<?>>) tupleTagToCoder.get(view.getTagInternal());
    Coder<WindowedValue<?>> elemCoder = coder.getElemCoder();

    final BoundedWindow sideInputWindow =
        view.getWindowMappingFn().getSideInputWindow(window);

    Path pattern = new Path(filePath + "*");
    try {
      FileSystem fs = pattern.getFileSystem(conf);
      FileStatus[] files = fs.globStatus(pattern);
      // TODO: handle empty views which may result in no files case.
      SequenceFile.Reader reader = new SequenceFile.Reader(fs, files[0].getPath(), conf);

      List<WindowedValue<?>> availableSideInputs = new ArrayList<>();
      BytesWritable value = new BytesWritable();
      while (reader.next(NullWritable.get(), value)) {
        ByteArrayInputStream inStream = new ByteArrayInputStream(value.getBytes());
        availableSideInputs.add(elemCoder.decode(inStream));
      }
      Iterable<WindowedValue<?>> sideInputForWindow =
          Iterables.filter(availableSideInputs, new Predicate<WindowedValue<?>>() {
            @Override
            public boolean apply(@Nullable WindowedValue<?> sideInputCandidate) {
              if (sideInputCandidate == null) {
                return false;
              }
              // first match of a sideInputWindow to the elementWindow is good enough.
              for (BoundedWindow sideInputCandidateWindow: sideInputCandidate.getWindows()) {
                if (sideInputCandidateWindow.equals(sideInputWindow)) {
                  return true;
                }
              }
              // no match found.
              return false;
            }
          });
      return view.getViewFn().apply(sideInputForWindow);
    } catch (IOException e) {
      Throwables.throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public <T> boolean contains(PCollectionView<T> view) {
    return tupleTagToFilePath.containsKey(view.getTagInternal());
  }

  @Override
  public boolean isEmpty() {
    return tupleTagToFilePath.isEmpty();
  }
}
