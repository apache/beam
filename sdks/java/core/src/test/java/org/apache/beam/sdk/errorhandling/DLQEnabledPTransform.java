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

/** Dummy PTransform that is configurable with a DLQ */
public class DLQEnabledPTransform extends PTransform<PCollection<Integer>, PCollection<Integer>> {

  private ErrorHandler<DeadLetter, ?> errorHandler = new NoOpErrorHandler<>();

  private DeadLetterHandler deadLetterHandler = DeadLetterHandler.THROWING_HANDLER;

  private static final TupleTag<Integer> RECORDS = new TupleTag<>();

  public DLQEnabledPTransform() {}

  public DLQEnabledPTransform withDeadLetterQueue(ErrorHandler<DeadLetter, ?> errorHandler) {
    this.errorHandler = errorHandler;
    this.deadLetterHandler = DeadLetterHandler.RECORDING_HANDLER;
    return this;
  }

  @Override
  public PCollection<Integer> expand(PCollection<Integer> input) {
    PCollectionTuple pCollectionTuple =
        input.apply(
            "NoOpDoFn",
            ParDo.of(new NoOpDoFn(deadLetterHandler))
                .withOutputTags(RECORDS, TupleTagList.of(DeadLetterHandler.DEAD_LETTER_TAG)));

    Coder<DeadLetter> deadLetterCoder;

    try {
      SchemaRegistry schemaRegistry = input.getPipeline().getSchemaRegistry();
      deadLetterCoder =
          SchemaCoder.of(
              schemaRegistry.getSchema(DeadLetter.class),
              TypeDescriptor.of(DeadLetter.class),
              schemaRegistry.getToRowFunction(DeadLetter.class),
              schemaRegistry.getFromRowFunction(DeadLetter.class));
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException(e);
    }

    errorHandler.addErrorCollection(
        pCollectionTuple.get(DeadLetterHandler.DEAD_LETTER_TAG).setCoder(deadLetterCoder));

    return pCollectionTuple.get(RECORDS).setCoder(BigEndianIntegerCoder.of());
  }

  public static class NoOpDoFn extends DoFn<Integer, Integer> {

    private DeadLetterHandler deadLetterHandler;

    public NoOpDoFn(DeadLetterHandler deadLetterHandler) {
      this.deadLetterHandler = deadLetterHandler;
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws Exception {
      Integer element = context.element();
      if (element % 2 == 0) {
        context.output(element);
      } else {
        deadLetterHandler.handle(
            context,
            element,
            BigEndianIntegerCoder.of(),
            new RuntimeException(),
            "Integer was odd",
            "NoOpDoFn");
      }
    }
  }
}
