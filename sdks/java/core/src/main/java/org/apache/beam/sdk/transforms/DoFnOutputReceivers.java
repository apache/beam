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
package org.apache.beam.sdk.transforms;

import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;

public class DoFnOutputReceivers {
  public static class WindowedContextOutputReceiver<T> implements OutputReceiver<T> {
    DoFn<?, T>.WindowedContext context;
    public WindowedContextOutputReceiver(DoFn<?, T>.WindowedContext context) {
      this.context = context;
    }

    @Override
    public void output(T output) {
      context.output(output);
    }

    @Override
    public void outputWithTimestamp(T output, Instant timestamp) {
      context.outputWithTimestamp(output, timestamp);
    }
  }

  public static class WindowedContextMultiOutputReceiver implements MultiOutputReceiver {
    DoFn<?, ?>.WindowedContext context;
    public WindowedContextMultiOutputReceiver(DoFn<?, ?>.WindowedContext context) {
      this.context = context;
    }

    @Override
    public <T> void output(TupleTag<T> tag, T output) {
      context.output(tag, output);
    }

    @Override
    public <T> void outputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      context.outputWithTimestamp(tag, output, timestamp);
    }
  }
}
