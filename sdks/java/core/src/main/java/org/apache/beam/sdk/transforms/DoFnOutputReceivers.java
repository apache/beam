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

import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;

/**
 * Common {@link OutputReceiver} and {@link MultiOutputReceiver} classes.
 */
public class DoFnOutputReceivers {
  /**
   * A {@link OutputReceiver} that delegates to a {@link DoFn.WindowedContext}.
   */
  public static class WindowedContextOutputReceiver<T> implements OutputReceiver<T> {
    DoFn<?, ?>.WindowedContext context;
    @Nullable TupleTag<T> outputTag;
    public WindowedContextOutputReceiver(DoFn<?, ?>.WindowedContext context,
                                         @Nullable TupleTag<T> outputTag) {
      this.context = context;
      this.outputTag = outputTag;
    }

    @Override
    public void output(T output) {
      if (outputTag != null) {
        context.output(outputTag, output);
      } else {
        ((DoFn<?, T>.WindowedContext) context).output(output);
      }
    }

    @Override
    public void outputWithTimestamp(T output, Instant timestamp) {
      if (outputTag != null) {
        context.outputWithTimestamp(outputTag, output, timestamp);
      } else {
        ((DoFn<?, T>.WindowedContext) context).outputWithTimestamp(output, timestamp);
      }
    }
  }

  /**
   * A {@link MultiOutputReceiver} that delegates to a {@link DoFn.WindowedContext}.
   */
  public static class WindowedContextMultiOutputReceiver implements MultiOutputReceiver {
    DoFn<?, ?>.WindowedContext context;
    public WindowedContextMultiOutputReceiver(DoFn<?, ?>.WindowedContext context) {
      this.context = context;
    }

    @Override
    public <T> OutputReceiver<T> get(TupleTag<T> tag) {
      return new WindowedContextOutputReceiver<T>(context, tag);
    }
  }
}
