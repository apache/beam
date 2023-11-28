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
package org.apache.beam.sdk.transforms.errorhandling;

import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler.DefaultErrorHandler;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

/**
 * Dummy PTransform that is configurable with a Bad Record Handler. TODO(johncasey) look to factor
 * some of this out for easy use in other IOs
 */
public class BRHEnabledPTransform extends PTransform<PCollection<Integer>, PCollection<Integer>> {

  private ErrorHandler<BadRecord, ?> errorHandler = new DefaultErrorHandler<>();

  private BadRecordRouter badRecordRouter = BadRecordRouter.THROWING_ROUTER;

  private static final TupleTag<Integer> RECORDS = new TupleTag<>();

  public BRHEnabledPTransform() {}

  public BRHEnabledPTransform withBadRecordHandler(ErrorHandler<BadRecord, ?> errorHandler) {
    this.errorHandler = errorHandler;
    this.badRecordRouter = BadRecordRouter.RECORDING_ROUTER;
    return this;
  }

  @Override
  public PCollection<Integer> expand(PCollection<Integer> input) {
    // TODO this pattern is a clunky. Look to improve this once we have ParDo level error handling.
    PCollectionTuple pCollectionTuple =
        input.apply(
            "NoOpDoFn",
            ParDo.of(new OddIsBad(badRecordRouter))
                .withOutputTags(RECORDS, TupleTagList.of(BadRecordRouter.BAD_RECORD_TAG)));

    errorHandler.addErrorCollection(
        pCollectionTuple
            .get(BadRecordRouter.BAD_RECORD_TAG)
            .setCoder(BadRecord.getCoder(input.getPipeline())));

    return pCollectionTuple.get(RECORDS).setCoder(BigEndianIntegerCoder.of());
  }

  public static class OddIsBad extends DoFn<Integer, Integer> {

    private final BadRecordRouter badRecordRouter;

    public OddIsBad(BadRecordRouter badRecordRouter) {
      this.badRecordRouter = badRecordRouter;
    }

    @ProcessElement
    public void processElement(@Element Integer element, MultiOutputReceiver receiver)
        throws Exception {
      if (element % 2 == 0) {
        receiver.get(RECORDS).output(element);
      } else {
        badRecordRouter.route(
            receiver,
            element,
            BigEndianIntegerCoder.of(),
            new RuntimeException("Integer was odd"),
            "Integer was odd");
      }
    }
  }
}
