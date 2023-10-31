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
package org.apache.beam.sdk.errorhandling;

import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.errorhandling.ErrorHandler.NoOpErrorHandler;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;

/** Dummy PTransform that is configurable with a Bad Record Handler. */
public class BRHEnabledPTransform extends PTransform<PCollection<Integer>, PCollection<Integer>> {

  private ErrorHandler<BadRecord, ?> errorHandler = new NoOpErrorHandler<>();

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
    PCollectionTuple pCollectionTuple =
        input.apply(
            "NoOpDoFn",
            ParDo.of(new NoOpDoFn(badRecordRouter))
                .withOutputTags(RECORDS, TupleTagList.of(BadRecordRouter.BAD_RECORD_TAG)));

    Coder<BadRecord> badRecordCoder;

    try {
      SchemaRegistry schemaRegistry = input.getPipeline().getSchemaRegistry();
      badRecordCoder =
          SchemaCoder.of(
              schemaRegistry.getSchema(BadRecord.class),
              TypeDescriptor.of(BadRecord.class),
              schemaRegistry.getToRowFunction(BadRecord.class),
              schemaRegistry.getFromRowFunction(BadRecord.class));
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException(e);
    }

    errorHandler.addErrorCollection(
        pCollectionTuple.get(BadRecordRouter.BAD_RECORD_TAG).setCoder(badRecordCoder));

    return pCollectionTuple.get(RECORDS).setCoder(BigEndianIntegerCoder.of());
  }

  public static class NoOpDoFn extends DoFn<Integer, Integer> {

    private BadRecordRouter badRecordRouter;

    public NoOpDoFn(BadRecordRouter badRecordRouter) {
      this.badRecordRouter = badRecordRouter;
    }

    @ProcessElement
    public void processElement(@Element Integer element, MultiOutputReceiver receiver) throws Exception {
      if (element % 2 == 0) {
        receiver.get(RECORDS).output(element);
      } else {
        badRecordRouter.route(
            receiver,
            element,
            BigEndianIntegerCoder.of(),
            new RuntimeException(),
            "Integer was odd",
            "NoOpDoFn");
      }
    }
  }
}
