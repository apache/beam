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
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/**
 * {@link Operation} that materializes input for group by key.
 */
public class ShuffleWriteOperation<T> extends Operation<T> {

  private final Coder<Object> keyCoder;
  private final Coder<Object> valueCoder;

  private transient TaskInputOutputContext<Object, Object, Object, Object> taskContext;

  public ShuffleWriteOperation(Coder<Object> keyCoder, Coder<Object> valueCoder) {
    super(0);
    this.keyCoder = checkNotNull(keyCoder, "keyCoder");
    this.valueCoder = checkNotNull(valueCoder, "valueCoder");
  }

  @Override
  public void start(TaskInputOutputContext<Object, Object, Object, Object> taskContext) {
    this.taskContext = checkNotNull(taskContext, "taskContext");
  }

  @Override
  public void process(WindowedValue<T> elem) throws IOException, InterruptedException {
    KV<?, ?> kv = (KV<?, ?>) elem.getValue();
    ByteArrayOutputStream keyStream = new ByteArrayOutputStream();
    keyCoder.encode(kv.getKey(), keyStream);

    ByteArrayOutputStream valueStream = new ByteArrayOutputStream();
    valueCoder.encode(kv.getValue(), valueStream);
    taskContext.write(new BytesWritable(keyStream.toByteArray()), valueStream.toByteArray());
  }
}
