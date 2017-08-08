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

import com.google.common.base.Throwables;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * Operation that writes to files.
 */
public class FileWriteOperation<T> extends Operation<T> {

  private final String fileName;
  private final Coder<WindowedValue<T>> coder;
  private transient MultipleOutputs mos;

  public FileWriteOperation(String fileName, Coder<WindowedValue<T>> coder) {
    super(0);
    this.fileName = checkNotNull(fileName, "fileName");
    this.coder = checkNotNull(coder, "coder");
  }

  @Override
  public void start(TaskInputOutputContext<Object, Object, Object, Object> taskContext) {
    this.mos = new MultipleOutputs(taskContext);
  }

  @Override
  public void process(WindowedValue<T> elem) throws IOException, InterruptedException {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    coder.encode(elem, stream);

    mos.write(fileName, NullWritable.get(), new BytesWritable(stream.toByteArray()));
  }

  @Override
  public void finish() {
    try {
      mos.close();
    } catch (Exception e) {
      Throwables.throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  public String getFileName() {
    return fileName;
  }
}
