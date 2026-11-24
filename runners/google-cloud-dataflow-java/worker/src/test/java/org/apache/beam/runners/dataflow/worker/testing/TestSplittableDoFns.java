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
package org.apache.beam.runners.dataflow.worker.testing;

import static org.apache.beam.runners.dataflow.util.Structs.addObject;
import static org.apache.beam.runners.dataflow.util.Structs.addString;

import java.util.Collections;
import java.util.Map;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.worker.util.WorkerPropertyNames;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.util.DoFnInfo;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.StringUtils;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;

/** Shared test {@link DoFn} implementations and serialization helpers for splittable DoFn tests. */
public final class TestSplittableDoFns {

  private TestSplittableDoFns() {}

  public static class StringSplittableDoFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext c, RestrictionTracker<OffsetRange, Long> tracker) {
      if (tracker.tryClaim(tracker.currentRestriction().getFrom())) {
        c.output(c.element());
      }
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(@SuppressWarnings("unused") @Element String element) {
      return new OffsetRange(0, 1);
    }
  }

  public static class IntegerSplittableDoFn extends DoFn<String, Integer> {
    @ProcessElement
    public void processElement(ProcessContext c, RestrictionTracker<OffsetRange, Long> tracker) {
      if (tracker.tryClaim(tracker.currentRestriction().getFrom())) {
        c.output(c.element().length());
      }
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(@SuppressWarnings("unused") @Element String element) {
      return new OffsetRange(0, 1);
    }
  }

  public static <OutputT> CloudObject createSplittableProcessFnCloudObject(
      DoFn<String, OutputT> fn, TupleTag<OutputT> mainOutputTag, Coder<OutputT> mainOutputCoder) {
    return createSplittableProcessFnCloudObject(
        fn, mainOutputTag, Collections.singletonMap(mainOutputTag, mainOutputCoder));
  }

  public static <OutputT> CloudObject createSplittableProcessFnCloudObject(
      DoFn<String, OutputT> fn,
      TupleTag<OutputT> mainOutputTag,
      Map<TupleTag<?>, Coder<?>> outputCoders) {
    String serializedFn =
        StringUtils.byteArrayToJsonString(
            SerializableUtils.serializeToByteArray(
                DoFnInfo.forFn(
                    fn,
                    WindowingStrategy.globalDefault(),
                    null /* side input views */,
                    StringUtf8Coder.of() /* input coder */,
                    outputCoders,
                    mainOutputTag,
                    DoFnSchemaInformation.create(),
                    Collections.emptyMap())));

    CloudObject cloudUserFn = CloudObject.forClassName("SplittableProcessFn");
    addString(cloudUserFn, PropertyNames.SERIALIZED_FN, serializedFn);
    addObject(
        cloudUserFn,
        WorkerPropertyNames.RESTRICTION_CODER,
        CloudObjects.asCloudObject(
            KvCoder.of(OffsetRange.Coder.of(), VoidCoder.of()), /* sdkComponents= */ null));
    return cloudUserFn;
  }
}
