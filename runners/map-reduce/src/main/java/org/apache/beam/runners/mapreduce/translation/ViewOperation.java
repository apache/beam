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
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/**
 * {@link Operation} that materializes views.
 */
public class ViewOperation<T> extends Operation<T> {

  private final Coder<WindowedValue<T>> valueCoder;

  private transient TaskInputOutputContext<Object, Object, Object, Object> taskContext;

  public ViewOperation(Coder<WindowedValue<T>> valueCoder) {
    super(0);
    this.valueCoder = checkNotNull(valueCoder, "valueCoder");
  }

  @Override
  public void start(TaskInputOutputContext<Object, Object, Object, Object> taskContext) {
    this.taskContext = checkNotNull(taskContext, "taskContext");
  }

  @Override
  public void process(WindowedValue<T> elem) {
    try {
      ByteArrayOutputStream valueStream = new ByteArrayOutputStream();
      valueCoder.encode(elem, valueStream);
      taskContext.write(new BytesWritable("view".getBytes()), valueStream.toByteArray());
    } catch (Exception e) {
      Throwables.throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }
}
