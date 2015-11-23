/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.cloud.dataflow.sdk.util.Structs.getString;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.WindowedValue.ValueOnlyWindowedValueCoder;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.Sink;

/**
 * Creates an AvroSink from a CloudObject spec.
 */
@SuppressWarnings("rawtypes")
public final class AvroSinkFactory {
  // Do not instantiate.
  private AvroSinkFactory() {}

  @SuppressWarnings("unused")
  public static <T> Sink<T> create(PipelineOptions options,
                                   CloudObject spec,
                                   Coder<T> coder,
                                   ExecutionContext executionContext,
                                   CounterSet.AddCounterMutator addCounterMutator)
      throws Exception {
    return create(spec, coder);
  }

  static <T> Sink<T> create(CloudObject spec, Coder<T> coder)
      throws Exception {
    String filename = getString(spec, PropertyNames.FILENAME);

    // Avro sinks are used both for outputting user data at the end of a pipeline and for
    // materializing PCollections as intermediate results. It is important to distinguish these
    // two cases because one requires only the values (outputting with AvroSink) and one requires
    // the values along with their window and timestamp (materializing intermediate results with
    // AvroByteSink).
    //
    // The logic we would like is "use AvroSink when writing at the end of a pipeline; use
    // AvroByteSink for materialized results".
    //
    // ValueOnlyWindowedValueCoder is used to decode/encode the values read from a Source, and used
    // to encode the values written to a Sink. FullWindowedValueCoder is used as the coder between
    // other edges in a Dataflow pipeline graph.
    //
    // Checking that the provided coder is an instance of ValueOnlyWindowedValueCoder is almost
    // enough to identify a user's AvroSink at the end of a pipeline, but it does not eliminate the
    // case when we are materializing immediately after reading from a Source. If this was the
    // entire check to decide to use AvroSink, there could be a crash when we materialized the
    // output of a Source that does not use AvroCoder, such as TextIO with StringUtf8Coder.
    //
    // Adding the additional test that the inner value coder is an AvroCoder will eliminate the
    // TextIO case but will leave sources that, like AvroSource, use AvroCoder to represent their
    // values. This fixes the potential crash, but still would use AvroSink for intermediate
    // results immediately after such a Source.
    //
    // Luckily, using AvroSink in these cases is safe. Though AvroSink will only encode the value,
    // and will drop the associated timestamp and window, the dropped values were applied by
    // ValueOnlyWindowedValueCoder and will be reapplied by the same when the file is re-read by
    // later in the pipeline.
    //
    // Otherwise, this is definitely a materialized result and we should use the AvroByteSink to
    // include the window and timestamp.
    //
    // See AvroReaderFactory#create for the accompanying reader logic.
    if (coder instanceof ValueOnlyWindowedValueCoder
        && ((ValueOnlyWindowedValueCoder) coder).getValueCoder() instanceof AvroCoder) {
      return new AvroSink(filename, (ValueOnlyWindowedValueCoder<?>) coder);
    } else {
      return new AvroByteSink<>(filename, coder);
    }
  }
}
